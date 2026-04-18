import { useRef, useEffect } from 'react'
import type { ActiveRoute } from '../types'
import { userHue } from '../utils'

interface Props {
  routes: ActiveRoute[]
}

const ROUTE_TTL_MS = 2200

function pt(id: string, side: 'right' | 'left' | 'center'): { x: number; y: number } | null {
  const el = document.getElementById(id)
  if (!el) return null
  const r = el.getBoundingClientRect()
  if (side === 'right')  return { x: r.right,  y: r.top + r.height / 2 }
  if (side === 'left')   return { x: r.left,   y: r.top + r.height / 2 }
  return { x: r.left + r.width / 2, y: r.top + r.height / 2 }
}

function ptWorkerEdge(id: string, side: 'left' | 'right'): { x: number; y: number } | null {
  const el = document.getElementById(id)
  if (!el) return null
  const r = el.getBoundingClientRect()
  const cx = r.left + r.width / 2
  const cy = r.top + r.height / 2
  const radius = r.width / 2
  return { x: side === 'left' ? cx - radius : cx + radius, y: cy }
}

export default function ConnectionLines({ routes }: Props) {
  const svgRef = useRef<SVGSVGElement>(null)
  const routesRef = useRef(routes)
  const rafRef = useRef<number>(0)
  const lastKnownRef = useRef<Map<string, { worker_idx: number; at: number }>>(new Map())

  routesRef.current = routes

  useEffect(() => {
    const draw = (now: number) => {
      rafRef.current = requestAnimationFrame(draw)
      const svg = svgRef.current
      if (!svg) return

      // Update last-known route per user
      for (const route of routesRef.current) {
        const existing = lastKnownRef.current.get(route.user_id)
        if (!existing || route.at > existing.at) {
          lastKnownRef.current.set(route.user_id, { worker_idx: route.worker_idx, at: route.at })
        }
      }

      const fragment = document.createDocumentFragment()

      for (const [user_id, { worker_idx, at }] of lastKnownRef.current) {
        const age = now - at
        const isActive = age < ROUTE_TTL_MS

        const waveformTip = pt(`waveform-tip-${user_id}`, 'right')
        const workerLeft  = ptWorkerEdge(`worker-${worker_idx}`, 'left')
        const workerRight = ptWorkerEdge(`worker-${worker_idx}`, 'right')
        const txLeft      = pt(`tx-${user_id}`, 'left')

        if (!waveformTip || !workerLeft || !workerRight) continue

        const hue = userHue(user_id)

        // Hot (< 500ms): pulse. Warm (500ms–TTL): fade down. Cold: permanent dim.
        let opacity: number
        const HOT_MS = 500
        if (age < HOT_MS) {
          opacity = 0.55 + 0.30 * Math.abs(Math.sin(now / 500))
        } else if (isActive) {
          const t = (age - HOT_MS) / (ROUTE_TTL_MS - HOT_MS)
          opacity = 0.55 * (1 - t) + 0.13 * t
        } else {
          opacity = 0.13
        }

        const stroke = `oklch(72% 0.2 ${hue} / ${opacity})`
        const strokeWidth = age < HOT_MS ? '1.8' : '1.2'

        const left = document.createElementNS('http://www.w3.org/2000/svg', 'path')
        left.setAttribute('d', `M ${waveformTip.x} ${waveformTip.y} L ${workerLeft.x} ${workerLeft.y}`)
        left.setAttribute('stroke', stroke)
        left.setAttribute('stroke-width', strokeWidth)
        left.setAttribute('fill', 'none')
        fragment.appendChild(left)

        if (txLeft) {
          const right = document.createElementNS('http://www.w3.org/2000/svg', 'path')
          right.setAttribute('d', `M ${workerRight.x} ${workerRight.y} L ${txLeft.x} ${txLeft.y}`)
          right.setAttribute('stroke', stroke)
          right.setAttribute('stroke-width', strokeWidth)
          right.setAttribute('fill', 'none')
          fragment.appendChild(right)
        }
      }

      svg.replaceChildren(fragment)
    }

    rafRef.current = requestAnimationFrame(draw)
    return () => cancelAnimationFrame(rafRef.current)
  }, [])

  return (
    <svg
      ref={svgRef}
      style={{
        position: 'fixed',
        inset: 0,
        width: '100vw',
        height: '100vh',
        pointerEvents: 'none',
        zIndex: 10,
        overflow: 'visible',
      }}
    />
  )
}
