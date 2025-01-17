import os
import xml.etree.ElementTree as ET
import mysql.connector

# Directory containing XML files
xml_directory = "/path/to/your/xml/files"

# MySQL connection
db = mysql.connector.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)
cursor = db.cursor()

# Create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS Product (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    version INT,
    singlecontent BOOLEAN
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Volume (
    id VARCHAR(255) PRIMARY KEY,
    product_id VARCHAR(255),
    name VARCHAR(255),
    number INT,
    sourcefiletype VARCHAR(50),
    preview_discid VARCHAR(255),
    preview_suffix VARCHAR(10),
    preview_installdir VARCHAR(255),
    preview_path_on_disc VARCHAR(255),
    preview_thumbnail_suffix VARCHAR(10),
    install_size_img INT,
    install_size_img_mov INT,
    total_preview_size INT,
    total_source_size FLOAT,
    base_j3_version VARCHAR(255),
    FOREIGN KEY (product_id) REFERENCES Product(id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Disc (
    id VARCHAR(255) PRIMARY KEY,
    volume_id VARCHAR(255),
    number INT,
    FOREIGN KEY (volume_id) REFERENCES Volume(id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Content (
    id VARCHAR(255) PRIMARY KEY,
    disc_id VARCHAR(255),
    type INT,
    name VARCHAR(255),
    originalfps INT,
    frames INT,
    description VARCHAR(255),
    resolution VARCHAR(50),
    resx INT,
    resy INT,
    base VARCHAR(255),
    keywords TEXT,
    FOREIGN KEY (disc_id) REFERENCES Disc(id)
)
""")

# Function to process a single XML file
def process_xml_file(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Extract and insert Product
    product = root.attrib
    product_id = product["id"]
    cursor.execute("""
    INSERT INTO Product (id, name, version, singlecontent)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE name=VALUES(name), version=VALUES(version), singlecontent=VALUES(singlecontent)
    """, (product["id"], product["name"], int(product["version"]), product["singlecontent"] == "true"))

    # Extract and insert Volume
    for volume in root.findall("volume"):
        volume_attrib = volume.attrib
        cursor.execute("""
        INSERT INTO Volume (id, product_id, name, number, sourcefiletype, preview_discid, preview_suffix, preview_installdir,
                            preview_path_on_disc, preview_thumbnail_suffix, install_size_img, install_size_img_mov,
                            total_preview_size, total_source_size, base_j3_version)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE name=VALUES(name), number=VALUES(number), sourcefiletype=VALUES(sourcefiletype),
                                preview_discid=VALUES(preview_discid), preview_suffix=VALUES(preview_suffix),
                                preview_installdir=VALUES(preview_installdir), preview_path_on_disc=VALUES(preview_path_on_disc),
                                preview_thumbnail_suffix=VALUES(preview_thumbnail_suffix), install_size_img=VALUES(install_size_img),
                                install_size_img_mov=VALUES(install_size_img_mov), total_preview_size=VALUES(total_preview_size),
                                total_source_size=VALUES(total_source_size), base_j3_version=VALUES(base_j3_version)
        """, (volume_attrib["id"], product_id, volume_attrib["name"], int(volume_attrib["number"]),
              volume_attrib["sourcefiletype"], volume_attrib["preview_discid"], volume_attrib["preview_suffix"],
              volume_attrib["preview_install_dir"], volume_attrib["preview_path_on_disc"],
              volume_attrib["preview_thumbnail_suffix"], int(volume_attrib["install_size_img"]),
              int(volume_attrib["install_size_img_mov"]), int(volume_attrib["totalPreviewSize"]),
              float(volume_attrib["totalSourceSize"]), volume_attrib["baseJ3Version"]))

        # Extract and insert Disc
        for disc in volume.findall("disc"):
            disc_attrib = disc.attrib
            cursor.execute("""
            INSERT INTO Disc (id, volume_id, number)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE volume_id=VALUES(volume_id), number=VALUES(number)
            """, (disc_attrib["id"], volume_attrib["id"], int(disc_attrib["number"])))

            # Extract and insert Content
            for content in disc.findall(".//content"):
                content_attrib = content.attrib
                keywords = content.find("keywords").text if content.find("keywords") is not None else ""
                cursor.execute("""
                INSERT INTO Content (id, disc_id, type, name, originalfps, frames, description, resolution, resx, resy, base, keywords)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE type=VALUES(type), name=VALUES(name), originalfps=VALUES(originalfps), frames=VALUES(frames),
                                        description=VALUES(description), resolution=VALUES(resolution), resx=VALUES(resx),
                                        resy=VALUES(resy), base=VALUES(base), keywords=VALUES(keywords)
                """, (content_attrib["id"], disc_attrib["id"], int(content_attrib["type"]), content_attrib.get("name", ""),
                      int(content_attrib["originalfps"]), int(content_attrib["frames"]), content_attrib["description"],
                      content_attrib["resolution"], int(content_attrib["resx"]), int(content_attrib["resy"]),
                      content_attrib.get("base", ""), keywords))

# Iterate over all XML files in the directory
for filename in os.listdir(xml_directory):
    if filename.endswith(".xml"):
        xml_path = os.path.join(xml_directory, filename)
        print(f"Processing file: {xml_path}")
        process_xml_file(xml_path)

# Commit and close
db.commit()
cursor.close()
db.close()

print("All XML files processed successfully!")

