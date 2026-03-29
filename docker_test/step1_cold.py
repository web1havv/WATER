import os

os.makedirs('/out/cold_extraction', exist_ok=True)
for i in range(5):
    path = f'/out/cold_extraction/patient_{i:03d}_{i*1000}.dcm'
    with open(path, 'wb') as f:
        f.write(b'\x00' * 128 + b'DICM')
        f.write(f'PatientID_{i:03d}'.encode().ljust(64))
    print(f'Created: {path}')
print('cold-extraction DONE. Files:', os.listdir('/out/cold_extraction'))
