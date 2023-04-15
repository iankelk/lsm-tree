def main():
    with open("output.txt", "w") as file:
        for i in range(1, 2000001):
            file.write(f"p {i*2} {i*2}\n")


if __name__ == "__main__":
    main()