def main():
    with open("output.txt", "w") as file:
        for i in range(1, 5000):
            file.write(f"p {i} {i}\n")


if __name__ == "__main__":
    main()