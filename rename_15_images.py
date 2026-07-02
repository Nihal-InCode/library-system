import os
import glob

images_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")

files = glob.glob(os.path.join(images_dir, "15*"))

if not files:
    print("No files starting with '15' found in images/")
else:
    counter = 0
    for filepath in sorted(files):
        old_name = os.path.basename(filepath)
        if counter == 0:
            new_name = "unknown.jpg"
        else:
            new_name = f"unknown_{counter}.jpg"
        
        new_path = os.path.join(images_dir, new_name)
        
        # If target already exists, skip to next number
        while os.path.exists(new_path):
            counter += 1
            new_name = f"unknown_{counter}.jpg"
            new_path = os.path.join(images_dir, new_name)
        
        os.rename(filepath, new_path)
        print(f"Renamed: {old_name} -> {new_name}")
        counter += 1

print(f"\nDone. Renamed {counter} file(s).")
