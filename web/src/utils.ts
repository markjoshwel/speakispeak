export function userHue(userId: string): number {
  let h = 5381
  for (let i = 0; i < userId.length; i++) {
    h = (((h << 5) + h) ^ userId.charCodeAt(i)) | 0
  }
  const raw = ((h % 360) + 360) % 360
  // Shift greenish 105-155 range away from background to avoid blending in
  return raw >= 105 && raw <= 155 ? (raw + 190) % 360 : raw
}

export function userColor(userId: string, lightness = 70, chroma = 0.18): string {
  return `oklch(${lightness}% ${chroma} ${userHue(userId)})`
}

export function userBorder(userId: string): string {
  return userColor(userId, 65, 0.2)
}

export function userBg(userId: string): string {
  return userColor(userId, 20, 0.06)
}
