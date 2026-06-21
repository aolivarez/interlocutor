#!/usr/bin/env bash
# make_test_audio.sh — generate per-callsign Opulent Voice test WAVs.
#
# Produces 48kHz mono 16-bit PCM WAVs (exactly what interlocutor's --audio-file
# wants, no conversion needed) using free/open-source TTS. Each station gets a
# distinct voice so the sources are actually distinguishable.
#
# Engines:
#   piper   (default) — neural, natural-sounding; auto-downloads voice models
#                       deps:  pip install piper-tts ; sudo apt install ffmpeg
#   espeak            — robotic but tiny/instant fallback
#                       deps:  sudo apt install espeak-ng ffmpeg
#
# Usage:
#   ./make_test_audio.sh [--engine piper|espeak] [OUTPUT_DIR]   # default: ./test_audio
#
# Then drive a transmitter per file:
#   python interlocutor.py KI5BAB-1 -p 57401 --audio-file test_audio/ki5bab-1.wav --loop-audio
#   python interlocutor.py W5NYV-2  -p 57402 --audio-file test_audio/w5nyv-2.wav  --loop-audio
set -euo pipefail

ENGINE="piper"
OUT="test_audio"
while [ $# -gt 0 ]; do
	case "$1" in
		--engine)   ENGINE="$2"; shift 2 ;;
		--engine=*) ENGINE="${1#*=}"; shift ;;
		-h|--help)  sed -n '2,18p' "$0"; exit 0 ;;
		*)          OUT="$1"; shift ;;
	esac
done
mkdir -p "$OUT"

# id | spoken name      (id -> output filename + interlocutor callsign;
#                        spoken name read phonetically by the TTS)
stations=(
	"ki5bab-1|Kilo India Five Bravo Alpha Bravo, station one"
	"w5nyv-2|Whiskey Five November Yankee Victor, station two"
	"kb5mu-3|Kilo Bravo Five Mike Uniform, station three"
	"nb0x-4|November Bravo Zero X-ray, station four"
)
# distinct voice per station (indexed alongside `stations`)
PIPER_VOICES=( en_US-lessac-medium en_US-amy-medium en_GB-alan-medium en_US-ryan-high )
ESPEAK_VOICES=( en+m1 en+f2 en+m3 en+f4 )

PIPER_VOICES_DIR="${PIPER_VOICES_DIR:-$HOME/.local/share/piper/voices}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "error: '$1' not found. $2" >&2; exit 1; }; }

# Fetch a piper voice (<onnx> + <onnx.json>) from HuggingFace if not already local.
download_piper_voice() {
	local v="$1"                       # e.g. en_US-lessac-medium
	local locale="${v%%-*}"            # en_US
	local rest="${v#*-}"               # lessac-medium
	local name="${rest%-*}"            # lessac
	local qual="${rest##*-}"           # medium
	local lang="${locale%%_*}"         # en
	local base="https://huggingface.co/rhasspy/piper-voices/resolve/main/${lang}/${locale}/${name}/${qual}/${v}"
	mkdir -p "$PIPER_VOICES_DIR"
	local ext
	for ext in onnx onnx.json; do
		if [ ! -f "$PIPER_VOICES_DIR/${v}.${ext}" ]; then
			echo "  downloading ${v}.${ext} ..."
			curl -fsSL "${base}.${ext}" -o "$PIPER_VOICES_DIR/${v}.${ext}"
		fi
	done
}

case "$ENGINE" in
	piper)  need ffmpeg "Install: sudo apt install ffmpeg"
	        need piper  "Install: pip install piper-tts"
	        need curl   "Install: sudo apt install curl" ;;
	espeak) need ffmpeg    "Install: sudo apt install ffmpeg"
	        need espeak-ng "Install: sudo apt install espeak-ng" ;;
	*)      echo "error: unknown engine '$ENGINE' (use: piper | espeak)" >&2; exit 1 ;;
esac

i=0
for s in "${stations[@]}"; do
	id="${s%%|*}"; spoken="${s#*|}"
	text="test. test. test. From ${spoken}, this is a test of the Opulent Voice \
digital voice and data protocol. Opulent Voice is a free and open source end to end \
voice protocol. test. test. test."

	if [ "$ENGINE" = "piper" ]; then
		voice="${PIPER_VOICES[$(( i % ${#PIPER_VOICES[@]} ))]}"
		download_piper_voice "$voice"
		printf '%s\n' "$text" | piper -m "$PIPER_VOICES_DIR/${voice}.onnx" -f "$OUT/.tmp.wav"
	else
		voice="${ESPEAK_VOICES[$(( i % ${#ESPEAK_VOICES[@]} ))]}"
		espeak-ng -v "$voice" -s 100 -w "$OUT/.tmp.wav" "$text"
	fi

	ffmpeg -y -loglevel error -i "$OUT/.tmp.wav" \
	       -ar 48000 -ac 1 -c:a pcm_s16le "$OUT/${id}.wav"
	echo "wrote $OUT/${id}.wav  (engine $ENGINE, voice $voice)"
	i=$((i + 1))
done
rm -f "$OUT/.tmp.wav"

echo "done -> $OUT/"
