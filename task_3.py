
def main():
  t=[[3-i for i in range(3)] for j in range(3)]
  print(t)
  s=0
  for i in range(3):
      print(t[i][i])
      s+=t[i][i]
  print(s)

#  var = 1
#  while var<10:
#     print("#")
#     var = var <<1


if __name__ == '__main__':
    main()
