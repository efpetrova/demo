import random

expected_winner_count = int(input())
last_sold_ticket = input().split()

def main():

    print(last_sold_ticket)
    ticket_last_number = int(last_sold_ticket[0])
    print(ticket_last_number)
    ticket_serial = last_sold_ticket[1].upper()
    print(ticket_serial)

    actual_winner_count = min(ticket_last_number, expected_winner_count)

    winners = random.sample(range(1, ticket_last_number + 1), actual_winner_count)

    for number in range(actual_winner_count):
        print(f"Победитель номер {actual_winner_count - number} - \"{winners[number]:06d} {ticket_serial}\"")

#random.randint(0,int(last_ticket[0:6]))
if __name__ == '__main__':
    main()
