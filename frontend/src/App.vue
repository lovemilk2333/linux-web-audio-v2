<script setup lang="ts">
import { ref, onUnmounted } from 'vue';
import { createDecoder } from "libopus-wasm";
import playerWorkletUrl from '/player.js?url';

const started = ref(false);

let audioContext: AudioContext | null = null;
let pcmWorkletNode: AudioWorkletNode | null = null;
let ws: WebSocket | null = null;
let decoder: Awaited<ReturnType<typeof createDecoder>> | null = null;

async function start() {
  if (started.value) {
    stop();
    return;
  }

  try {
    decoder = await createDecoder();

    audioContext = new AudioContext({ sampleRate: 48000 });
    if (audioContext.state === 'suspended') {
      await audioContext.resume();
    }

    await audioContext.audioWorklet.addModule(playerWorkletUrl);

    pcmWorkletNode = new AudioWorkletNode(audioContext, 'pcm-player-processor', {
      numberOfInputs: 0,
      numberOfOutputs: 1,
      outputChannelCount: [2]
    });
    pcmWorkletNode.connect(audioContext.destination);

    const wsUrl = new URL('/backend/stream', location.href);
    wsUrl.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    
    ws = new WebSocket(wsUrl);
    ws.binaryType = 'arraybuffer';

    ws.onmessage = (event: MessageEvent<ArrayBuffer>) => {
      if (!decoder || !pcmWorkletNode) return;

      const packet = new Uint8Array(event.data);

      // 解码为交错的 Float32PCM 数组: [L0, R0, L1, R1, ...]
      const interleavedFloat = decoder.decodeFloat(packet);
      if (!interleavedFloat || interleavedFloat.length === 0) return;

      // 零拷贝直接全量传给 Worklet
      pcmWorkletNode.port.postMessage(
        {
          type: "PCM_DATA",
          interleaved: interleavedFloat,
        },
        [interleavedFloat.buffer]
      );
    };

    started.value = true;
  } catch (err) {
    console.error('Failed to start:', err);
    stop();
  }
}

function stop() {
  started.value = false;

  if (ws) {
    ws.onmessage = null;
    ws.close();
    ws = null;
  }

  if (pcmWorkletNode) {
    pcmWorkletNode.disconnect();
    pcmWorkletNode = null;
  }

  if (audioContext) {
    audioContext.close();
    audioContext = null;
  }

  if (decoder) {
    decoder.free();
    decoder = null;
  }
}

onUnmounted(() => {
  stop();
});
</script>

<template>
  <button @click="start">
    {{ !started ? 'Start Stream' : 'Stop Stream' }}
  </button>
</template>