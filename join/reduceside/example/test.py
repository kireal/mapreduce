a = [1, 2, 3, 4, 5, 6]
idx = [1, 2, 3]
#print '%s,%s,%s' %([a[int(i)] for i in idx])
b = [str(a[int(i)]) for i in idx]
print '\t'.join(b) + '\t' + ';'.join(b)

line = "1;2;3;4;5;6;7"
LeftDSKeyIDX = "2,3,4"
SelectLeftIDX = "5,6"
split_line = line.split(";") # split record
keyidx = LeftDSKeyIDX.split(",")
selidx = SelectLeftIDX.split(",")
key = "" 
value = ""
key = [split_line[int(i)] for i in keyidx] # get key from record
value = [split_line[int(i)] for i in selidx] # get velue from record
value.insert(0,"L")
print key
print value