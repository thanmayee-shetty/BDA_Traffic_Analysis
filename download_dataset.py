import kagglehub

path = kagglehub.dataset_download(
    "sobhanmoosavi/us-accidents",
    output_dir="./dataset/raw"
)

print("Dataset downloaded successfully!")
print("Location:", path)