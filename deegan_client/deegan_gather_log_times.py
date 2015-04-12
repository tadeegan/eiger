import sys

def main(args):
    numbers = []
    for line in open(args[0]):
        if("Completion Complete" in line):
            i = line.index("e (") + 3
            j = line.index("ms")
            numString = line[i:j]
            numbers.append(int(numString))
    print numbers

if __name__ == "__main__":
   main(sys.argv[1:])
