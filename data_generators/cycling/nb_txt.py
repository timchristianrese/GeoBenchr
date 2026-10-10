import os
# print number of txt file in a directory
directory = './output/all/'
txt_count = 0
for filename in os.listdir(directory):
    if filename.endswith('.txt'):
        txt_count += 1
print(f'Nombre de fichiers .txt : {txt_count}')