import { useState, useEffect, useRef } from 'react'

const SPRITE_COUNT = 31

interface Props {
  triggerAt: number
}

export default function SpeakiSprite({ triggerAt }: Props) {
  const [spriteNum] = useState(
    () => String(Math.floor(Math.random() * SPRITE_COUNT) + 1).padStart(2, '0'),
  )
  const [hops, setHops] = useState(0)
  const prevTrigger = useRef(triggerAt)

  useEffect(() => {
    if (triggerAt === 0 || triggerAt === prevTrigger.current) return
    prevTrigger.current = triggerAt
    setHops((n) => n + 1)
  }, [triggerAt])

  const hopping = hops > 0

  return (
    <div
      className={`speaki-sprite${hopping ? ' speaki-sprite--hop' : ' speaki-sprite--idle'}`}
      onAnimationEnd={() => setHops((n) => Math.max(0, n - 1))}
    >
      <img
        src={`/assets/sprites/speaki_${spriteNum}.png`}
        alt="speaki"
        draggable={false}
      />
    </div>
  )
}
