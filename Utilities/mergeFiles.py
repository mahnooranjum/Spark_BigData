
# f = open("f_11G.csv", "r")
# print(f.readline())
# f.close()

filenames = ['file1.csv', 'file2.csv']

with open('file3.csv', 'w') as outfile:
    for i in filenames:
        with open(i) as infile:
            outfile.write(infile.read())
        
        outfile.write('\n')
            