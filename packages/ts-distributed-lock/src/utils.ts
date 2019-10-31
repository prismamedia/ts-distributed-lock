export async function sleep(ms: number): Promise<true> {
  return new Promise<true>(resolve => setTimeout(resolve, ms, true));
}
