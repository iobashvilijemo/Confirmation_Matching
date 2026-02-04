import os
from pathlib import Path
import fitz

def list_files(folder_path):
    """List all files in a folder."""
    return [
        f for f in os.listdir(folder_path)
        if os.path.isfile(os.path.join(folder_path, f))
    ]

def extract_text_from_pdfs(input_folder):
    """Extract text from all PDF files in a folder."""
    files = list_files(input_folder)
    documents = []
    
    try:
        for file in files:
            file_path = os.path.join(input_folder, file)
            msg = fitz.open(file_path)
            documents.append(msg)
    except Exception as e:
        print(f"Error opening files: {e}")
    
    # Extract text from all pages
    details = []
    for doc in documents:
        for page in doc:
            text = page.get_text("text")
            details.append(text)
    
    return details

def save_texts_as_txt(text_list, output_dir):
    """Save text list to individual txt files."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for idx, text in enumerate(text_list, start=1):
        file_path = output_dir / f"dummy_{idx}.txt"
        file_path.write_text(text, encoding="utf-8")
    
    print(f"✓ Successfully saved {len(text_list)} files to {output_dir}")

if __name__ == "__main__":
    input_folder = "./test/Exports_2026-01-30_15-22-57"
    output_folder = "./External_Data/dummy"
    
    # Extract text from PDFs
    print(f"Extracting text from PDFs in {input_folder}...")
    extracted_text = extract_text_from_pdfs(input_folder)
    
    # Save to txt files
    print(f"Saving extracted text to {output_folder}...")
    save_texts_as_txt(extracted_text, output_folder)
