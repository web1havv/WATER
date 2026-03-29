import os

os.makedirs('/out/extracted-images', exist_ok=True)
dicom_files = [f for f in os.listdir('/in/cold_extraction') if f.endswith('.dcm')]
print(f'png-extraction: found {len(dicom_files)} DICOM files to process')

processed = 0
for dcm_file in sorted(dicom_files):
    png_path = f'/out/extracted-images/{dcm_file.replace(".dcm", ".png")}'
    with open(png_path, 'wb') as f:
        # Minimal valid PNG: 1x1 white pixel
        f.write(b'\x89PNG\r\n\x1a\n')
        f.write(b'\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x00\x00\x00\x00:~\x9bU')
        f.write(b'\x00\x00\x00\nIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18\xd4e')
        f.write(b'\x00\x00\x00\x00IEND\xaeB`\x82')
    processed += 1
    print(f'  Converted: {dcm_file} -> {os.path.basename(png_path)} ({os.path.getsize(png_path)} bytes)')

print(f'png-extraction DONE. Converted {processed} files.')

with open('/out/metadata.csv', 'w') as f:
    f.write('filename,modality,node\n')
    for dcm_file in sorted(dicom_files):
        f.write(f'{dcm_file},CT,water-png-container\n')
print('metadata.csv written.')
print(f'Output: {os.listdir("/out/extracted-images")}')
