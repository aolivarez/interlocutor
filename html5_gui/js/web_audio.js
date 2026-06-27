// Browser audio I/O for --web-audio mode.
//
//   TX: getUserMedia -> pcm-capture worklet -> 1920-sample int16 frames ->
//       binary WebSocket, but only while PTT is held.
//   RX: binary PCM frames from the server -> jitter-buffered Web Audio playback.
//
// One AudioContext @ 48 kHz serves both. Mobile/desktop autoplay policy requires
// it to be created/resumed from a user gesture, so the first PTT (TX) or the
// first tap/keypress (RX) unlocks it.
(function () {
	var SR = 48000;
	var ctx = null, rxGain = null, rxNextTime = 0;
	var RX_PREROLL = 0.10;            // 100 ms jitter cushion before first frame

	var micStream = null, micSrc = null, micNode = null;
	var workletLoaded = false, acquiring = false, streaming = false;

	function ensureCtx() {
		if (!ctx) {
			ctx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: SR });
			rxGain = ctx.createGain();
			rxGain.gain.value = 1.0;
			rxGain.connect(ctx.destination);
			// Keep-alive: a silent, always-on source so the output device stays
			// active even when the mic is released. Without this the context idles
			// when PTT is off and RX playback would only work while transmitting.
			try {
				var keepAlive = ctx.createConstantSource();
				keepAlive.offset.value = 0;
				keepAlive.connect(rxGain);
				keepAlive.start();
			} catch (e) { /* ConstantSource unsupported — ignore */ }
			if (ctx.sampleRate !== SR) {
				console.warn('web-audio: AudioContext is ' + ctx.sampleRate +
				             ' Hz (wanted 48000) — audio may be off-pitch on this device');
			}
		}
		if (ctx.state === 'suspended') { ctx.resume().catch(function () {}); }
		return ctx;
	}

	// ---- RX: real-time, jitter-buffered playback of incoming PCM frames ----
	var RX_MAXLEAD = 0.4;            // cap scheduled latency; resync if we drift past it
	window.webAudioPlayRx = function (arrayBuffer) {
		if (!ctx) return;
		if (ctx.state !== 'running') {
			// Got suspended (tab backgrounded / power policy) — try to recover so
			// playback resumes instead of silently dying after a while.
			ctx.resume().catch(function () {});
			return;
		}
		var pcm = new Int16Array(arrayBuffer);
		if (!pcm.length) return;
		// 1920 samples = mono (single channel); 3840 = interleaved stereo (mixer).
		var stereo = (pcm.length === 1920 * 2);
		var n = stereo ? (pcm.length >> 1) : pcm.length;
		var ab = ctx.createBuffer(stereo ? 2 : 1, n, SR);
		if (stereo) {
			var L = ab.getChannelData(0), R = ab.getChannelData(1);
			for (var i = 0; i < n; i++) { L[i] = pcm[2 * i] / 32768; R[i] = pcm[2 * i + 1] / 32768; }
		} else {
			var m = ab.getChannelData(0);
			for (var j = 0; j < n; j++) m[j] = pcm[j] / 32768;
		}
		var src = ctx.createBufferSource();
		src.buffer = ab;
		src.connect(rxGain);
		var now = ctx.currentTime;
		// Resync if we've drifted out of the [now+5ms, now+MAXLEAD] window — either
		// underflow (fell behind) OR runaway latency from server/browser clock skew
		// accumulating over a long session (the "goes silent after a while" bug).
		if (rxNextTime < now + 0.005 || rxNextTime > now + RX_MAXLEAD) {
			rxNextTime = now + RX_PREROLL;
		}
		src.start(rxNextTime);
		rxNextTime += ab.duration;                       // schedule frames back-to-back
	};

	// ---- audio unlock (autoplay/mobile: must be a user gesture) ----
	window.webAudioUnlock = function () {
		ensureCtx();
		return !!(ctx && ctx.state === 'running');
	};

	// ---- TX: microphone capture ----
	function sendFrame(buf) {
		if (streaming && typeof ws !== 'undefined' && ws && ws.readyState === WebSocket.OPEN) {
			ws.send(buf);   // ArrayBuffer: 3840 bytes (1920 int16 samples)
		}
	}

	// Acquire the microphone and wire it to the capture worklet. Called on PTT-on
	// so the browser's "mic in use" indicator only lights while transmitting.
	async function micAcquire() {
		if (micStream || acquiring) return !!micStream;
		// Mic API is only exposed in a secure context (HTTPS or localhost).
		if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
			var msg = 'Microphone unavailable: open this page over HTTPS or via ' +
			          'http://localhost (e.g. an SSH tunnel). isSecureContext=' +
			          window.isSecureContext;
			console.error('web-audio:', msg);
			if (typeof showNotification === 'function') showNotification(msg, 'error');
			return false;
		}
		acquiring = true;
		try {
			ensureCtx();
			if (!workletLoaded) {
				await ctx.audioWorklet.addModule('/static/js/pcm-capture-worklet.js');
				workletLoaded = true;
			}
			micStream = await navigator.mediaDevices.getUserMedia({
				audio: {
					channelCount: 1, sampleRate: SR,
					echoCancellation: true, noiseSuppression: true, autoGainControl: true
				}
			});
			micSrc = ctx.createMediaStreamSource(micStream);
			micNode = new AudioWorkletNode(ctx, 'pcm-capture');
			micNode.port.onmessage = function (e) { sendFrame(e.data); };
			micSrc.connect(micNode);
			// keep the worklet running without monitoring our own mic
			var muted = ctx.createGain();
			muted.gain.value = 0;
			micNode.connect(muted);
			muted.connect(ctx.destination);
			console.log('web-audio mic acquired @ ' + ctx.sampleRate + ' Hz');
		} catch (err) {
			console.error('web-audio mic acquire failed:', err);
			if (typeof showNotification === 'function') {
				showNotification('Microphone access failed: ' + (err && err.message || err), 'error');
			}
			micStream = null;
		}
		acquiring = false;
		return !!micStream;
	}

	// Release the mic: stop streaming, tear down the nodes, and STOP the device
	// tracks — that last step is what turns off the browser mic indicator.
	function micRelease() {
		streaming = false;
		try { if (micSrc) micSrc.disconnect(); } catch (e) {}
		try { if (micNode) micNode.disconnect(); } catch (e) {}
		micSrc = null; micNode = null;
		if (micStream) {
			micStream.getTracks().forEach(function (t) { try { t.stop(); } catch (e) {} });
			micStream = null;
		}
	}

	window.webAudioStartTx = async function () {
		ensureCtx();
		if (!micStream) { if (!(await micAcquire())) return false; }
		streaming = true;
		return true;
	};
	window.webAudioStopTx = function () { micRelease(); };   // PTT off -> release device
	window.webAudioIsReady = function () { return !!micStream; };

	// Enable Audio button: unlock playback and grant mic permission up front
	// (prompt on an explicit click), then release so the mic isn't held until PTT.
	window.webAudioRequestMic = async function () {
		ensureCtx();
		var ok = await micAcquire();
		micRelease();          // permission now granted; don't hold the device
		return ok;
	};

	// First user gesture unlocks audio so RX plays even without keying PTT.
	function gestureUnlock() { if (window.webAudioMode) ensureCtx(); }
	['pointerdown', 'keydown', 'touchstart'].forEach(function (ev) {
		document.addEventListener(ev, gestureUnlock, { passive: true });
	});
})();
