import { useRef, useEffect, memo } from 'react'

interface Props {
  history: number[]
  hue: number
  userId: string
}

const BARS = 50
const TICK_MS = 50          // advance at ~20fps
const LIVE_WINDOW_MS = 120  // treat audio as live for 120ms after last event
const TAPER_START = 0.82    // full-height for first 82% of bars

function WaveformCanvas({ history, hue, userId }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const histRef = useRef(history)
  const lastLiveRef = useRef(0)
  const bufRef = useRef<number[]>(new Array(BARS).fill(0))
  const lastTickRef = useRef(0)
  const rafRef = useRef(0)

  // Always keep histRef current without triggering effects
  histRef.current = history

  // Track the timestamp of the last live_audio event
  useEffect(() => {
    lastLiveRef.current = performance.now()
  }, [history])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return

    const drawFrame = (buf: number[]) => {
      const ctx = canvas.getContext('2d')
      if (!ctx) return
      const dpr = window.devicePixelRatio || 1
      const cssW = canvas.clientWidth || 160
      const cssH = canvas.clientHeight || 28
      if (canvas.width !== cssW * dpr || canvas.height !== cssH * dpr) {
        canvas.width = cssW * dpr
        canvas.height = cssH * dpr
        ctx.scale(dpr, dpr)
      }
      ctx.clearRect(0, 0, cssW, cssH)
      const barW = cssW / BARS
      const gap = 1.5
      for (let i = 0; i < BARS; i++) {
        const amp = buf[i] ?? 0
        const posNorm = i / BARS
        const iNorm = (posNorm - TAPER_START) / (1 - TAPER_START)
        const taper = posNorm < TAPER_START ? 1.0 : Math.pow(Math.max(0, 1 - iNorm), 0.65)
        const barH = Math.max(2, amp * cssH * 0.88 * taper)
        const x = i * barW
        const y = (cssH - barH) / 2
        const alpha = 0.32 + 0.68 * taper
        ctx.fillStyle = `oklch(72% 0.2 ${hue} / ${alpha})`
        ctx.beginPath()
        ctx.roundRect(x + gap / 2, y, barW - gap, barH, 2)
        ctx.fill()
      }
    }

    const tick = (now: number) => {
      rafRef.current = requestAnimationFrame(tick)
      if (now - lastTickRef.current < TICK_MS) return
      lastTickRef.current = now

      // When live audio is active, push the latest amplitude.
      // After LIVE_WINDOW_MS of silence, push zeros so the waveform scrolls to flat.
      const isLive = now - lastLiveRef.current < LIVE_WINDOW_MS
      const sample = isLive ? (histRef.current[0] ?? 0) : 0
      const newBuf = [sample, ...bufRef.current.slice(0, BARS - 1)]
      bufRef.current = newBuf
      drawFrame(newBuf)
    }

    rafRef.current = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(rafRef.current)
  }, [hue])

  return <canvas ref={canvasRef} className="waveform-canvas" id={`waveform-tip-${userId}`} />
}

export default memo(WaveformCanvas)
