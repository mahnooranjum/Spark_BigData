
# f = open("f_11G.csv", "r")
# print(f.readline())
# f.close()

with open('f_11G.csv', 'r') as istr:
    i = 1
    with open('f_11Gi.csv', 'w') as ostr:
        for line in istr:
            temp = line.rstrip('\n') + "," + str(i) 
            i = i+1
            print(temp, file=ostr)
            