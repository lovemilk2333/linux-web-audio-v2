const MAX_BUFFERED_SAMPLES = 24000; // 500 ms at 48 kHz
const PREFILL_SAMPLES = 960;
const FADE_SAMPLES = 64;

class PCMPlayerProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.packetQueue = [];
    this.currentPacket = null;
    this.currentOffset = 0;
    this.bufferedSamples = 0;
    this.samplesSinceReport = 0;
    this.samplesPerPacket = 0;
    this.lastReceivedSequence = null;
    this.primed = false;
    this.fadeIn = 0;
    this.fadeOut = 0;
    this.lastLeft = 0;
    this.lastRight = 0;
    this.underruns = 0;
    this.droppedPackets = 0;

    this.port.onmessage = (event) => {
      const { type, interleaved, sequence } = event.data;
      if (type === "CLEAR") {
        this.packetQueue = [];
        this.currentPacket = null;
        this.currentOffset = 0;
        this.bufferedSamples = 0;
        this.primed = false;
        this.fadeIn = 0;
        this.fadeOut = 0;
        this.lastLeft = 0;
        this.lastRight = 0;
        return;
      }
      if (type !== "PCM_DATA" || !interleaved || interleaved.length === 0 || interleaved.length % 2 !== 0) return;

      this.packetQueue.push(interleaved);
      this.lastReceivedSequence = sequence;
      this.samplesPerPacket = interleaved.length / 2;
      this.bufferedSamples += this.samplesPerPacket;
      while (this.bufferedSamples > MAX_BUFFERED_SAMPLES && this.packetQueue.length > 1) {
        const discarded = this.packetQueue.shift();
        this.bufferedSamples -= discarded.length / 2;
        this.droppedPackets++;
      }
    };
  }

  process(inputs, outputs) {
    const output = outputs[0];
    const leftChannel = output[0];
    const rightChannel = output[1];
    if (!leftChannel || !rightChannel) return true;

    for (let i = 0; i < leftChannel.length; i++) {
      if (!this.primed && this.fadeOut === 0 && this.bufferedSamples >= PREFILL_SAMPLES) {
        this.primed = true;
        this.fadeIn = FADE_SAMPLES;
      }

      if (this.fadeOut > 0) {
        const scale = (this.fadeOut - 1) / FADE_SAMPLES;
        leftChannel[i] = this.lastLeft * scale;
        rightChannel[i] = this.lastRight * scale;
        this.fadeOut--;
        continue;
      }

      if (!this.primed) continue;

      if (!this.currentPacket) {
        this.currentPacket = this.packetQueue.shift() || null;
        this.currentOffset = 0;
      }
      if (!this.currentPacket) {
        this.primed = false;
        this.fadeIn = 0;
        this.fadeOut = FADE_SAMPLES;
        this.underruns++;
        i--;
        continue;
      }

      const packet = this.currentPacket;
      const scale = this.fadeIn > 0 ? (FADE_SAMPLES - this.fadeIn + 1) / FADE_SAMPLES : 1;
      leftChannel[i] = packet[this.currentOffset] * scale;
      rightChannel[i] = packet[this.currentOffset + 1] * scale;
      if (this.fadeIn > 0) this.fadeIn--;
      this.lastLeft = leftChannel[i];
      this.lastRight = rightChannel[i];
      this.currentOffset += 2;
      this.bufferedSamples--;

      if (this.currentOffset >= packet.length) {
        this.currentPacket = null;
      }
    }

    this.samplesSinceReport += leftChannel.length;
    if (this.samplesSinceReport >= 4800) {
      this.samplesSinceReport = 0;
      this.port.postMessage({
        type: "BUFFER_STATUS",
        bufferedPackets: this.samplesPerPacket ? Math.ceil(this.bufferedSamples / this.samplesPerPacket) : 0,
        sequence: this.lastReceivedSequence,
        underruns: this.underruns,
        droppedPackets: this.droppedPackets,
      });
    }
    return true;
  }
}

registerProcessor("pcm-player-processor", PCMPlayerProcessor);
