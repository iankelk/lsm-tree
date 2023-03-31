def main():
    with open("get.txt", "w") as file:
        for i in range(1, 7000):
            file.write(f"g {i} {i}\n")


if __name__ == "__main__":
    main()