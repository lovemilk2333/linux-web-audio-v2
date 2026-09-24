const MAX_BUFFERED_SAMPLES = 24000; // 500 ms at 48 kHz
const FADE_SAMPLES = 256;
const SAMPLES_PER_FRAME = 120;

class PCMPlayerProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.packetQueue = [];
    this.currentPacket = null;
    this.currentOffset = 0;
    this.bufferedSamples = 0;
    this.samplesSinceReport = 0;
    this.samplesPerPacket = 0;
    this.lowWatermarkSamples = SAMPLES_PER_FRAME;
    this.prefillSamples = SAMPLES_PER_FRAME * 16;
    this.lowReportArmed = true;
    this.lastReceivedSequence = null;
    this.primed = false;
    this.fadeIn = 0;
    this.fadeOut = 0;
    this.lastLeft = 0;
    this.lastRight = 0;
    this.underruns = 0;
    this.droppedPackets = 0;

    this.port.onmessage = (event) => {
      const { type, interleaved, sequence, lowerFrames, targetFrames } = event.data;
      if (type === "SET_BUFFER_LIMIT") {
        this.lowWatermarkSamples = Math.max(1, lowerFrames) * SAMPLES_PER_FRAME;
        this.prefillSamples = Math.min(8, Math.max(targetFrames, lowerFrames)) * SAMPLES_PER_FRAME;
        this.lowReportArmed = this.bufferedSamples > this.lowWatermarkSamples;
        this.maybeReportLowBuffer();
        return;
      }
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
        this.lowReportArmed = true;
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
      if (this.bufferedSamples > this.lowWatermarkSamples) this.lowReportArmed = true;
      this.maybeReportLowBuffer();
    };
  }

  postBufferStatus(lowWatermark = false) {
    this.port.postMessage({
      type: "BUFFER_STATUS",
      bufferedFrames: Math.ceil(this.bufferedSamples / SAMPLES_PER_FRAME),
      sequence: this.lastReceivedSequence,
      underruns: this.underruns,
      droppedPackets: this.droppedPackets,
      lowWatermark,
    });
  }

  maybeReportLowBuffer() {
    if (this.lowReportArmed && this.lastReceivedSequence !== null && this.bufferedSamples <= this.lowWatermarkSamples) {
      this.lowReportArmed = false;
      this.postBufferStatus(true);
    }
  }

  process(inputs, outputs) {
    const output = outputs[0];
    const leftChannel = output[0];
    const rightChannel = output[1];
    if (!leftChannel || !rightChannel) return true;

    for (let i = 0; i < leftChannel.length; i++) {
      if (!this.primed && this.fadeOut === 0 && this.bufferedSamples >= this.prefillSamples) {
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
      const scale = this.fadeIn > 0
        ? Math.sin(((FADE_SAMPLES - this.fadeIn + 1) / FADE_SAMPLES) * Math.PI / 2)
        : 1;
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

    this.maybeReportLowBuffer();

    this.samplesSinceReport += leftChannel.length;
    if (this.samplesSinceReport >= 4800) {
      this.samplesSinceReport = 0;
      this.postBufferStatus();
    }
    return true;
  }
}

registerProcessor("pcm-player-processor", PCMPlayerProcessor);
