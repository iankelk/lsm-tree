def main():
    with open("output.txt", "w") as file:
        for i in range(1, 1000001):
            file.write(f"p {i} {i}\n")


if __name__ == "__main__":
    main()