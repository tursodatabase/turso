const tursoWasmBase64 = '__PLACEHOLDER__';
async function convertBase64ToBinary(base64Url: string): Promise<ArrayBuffer> {
    const blob = await fetch(base64Url).then(res => res.blob());
    return await blob.arrayBuffer();
}

export async function tursoWasm(): Promise<ArrayBuffer> {
    return await convertBase64ToBinary(tursoWasmBase64);
}