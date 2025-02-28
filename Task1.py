def main():
    input_one = input('Enter one:').lower()
    input_two = input('Enter two:').lower()
    result = []
    input_two = ''.join([j for i,j in enumerate(input_two) if j not in input_two[:i]])
    print(input_two)

    if isEnglish(input_one) and isEnglish(input_two):
        for i in input_two:
            result = []
            if i not in input_one:
                result=[None]
            for j in range(len(input_one)):
                if i == input_one[j]:
                    result.append(j + 1)
            print(i," ".join(str(x) for x in result))

    else:
        print("Check language")


def isEnglish(s):
    return s.isascii()


if __name__ == '__main__':
    main()
