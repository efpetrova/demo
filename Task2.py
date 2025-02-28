import re
def main():
    str_input = input().lower()
    result = []

    for i in str_input:
        if i not in result and re.compile(r'[a-z]').match(i):
            result.append(i)
    result.sort()
    print(" ".join(str(x) for x in result))



if __name__ == '__main__':
    main()