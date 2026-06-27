// AudioWorklet for --web-audio TX: accumulate the mic into 1920-sample (40 ms
// @ 48 kHz) frames, convert float32 -> signed 16-bit PCM, and post each frame's
// ArrayBuffer (3840 bytes) to the main thread, which ships it over the
// WebSocket. Matches the OPV protocol frame the server feeds to the encoder.
class PCMCapture extends AudioWorkletProcessor {
	constructor() {
		super();
		this.frame = new Int16Array(1920);
		this.n = 0;
	}

	process(inputs) {
		const input = inputs[0];
		if (!input || !input[0]) return true;   // no mic data this block
		const ch = input[0];                     // mono (channel 0)
		for (let i = 0; i < ch.length; i++) {
			let s = ch[i];
			if (s > 1) s = 1; else if (s < -1) s = -1;
			this.frame[this.n++] = s < 0 ? (s * 0x8000) : (s * 0x7FFF);
			if (this.n === 1920) {
				const out = new Int16Array(this.frame);          // copy
				this.port.postMessage(out.buffer, [out.buffer]); // transfer
				this.n = 0;
			}
		}
		return true;                             // keep processor alive
	}
}
registerProcessor('pcm-capture', PCMCapture);
