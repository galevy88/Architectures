from PIL import Image

img = Image.open("img1.png")

img = img.convert("RGBA")
data = img.getdata()

new_data = []
for item in data:
    if item[0] > 200 and item[1] > 200 and item[2] > 200:
        new_data.append((255, 255, 255, 255))
    else:
        new_data.append(item)

# Update image data
img.putdata(new_data)

# Save the modified image
img.save("img1_white.png")
img.show()
