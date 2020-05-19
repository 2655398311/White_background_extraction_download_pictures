import cv2
import os
import numpy
import shutil

gallery = open("config.txt","r").read()[5:].strip()

def get_white_percent(img):
    if img is None: # 部分图形是空的，尺寸为0*0
        return 0.0
    return round(numpy.sum(img >= 255) / img.shape[0] / img.shape[1], 3) # 返回白色像素比例


def start():
    # 检查是否已经存在分离后的文件夹，若不存在，创建对应文件夹
    if f"{gallery}_not_white" not in os.listdir():
        os.mkdir(f"{gallery}_not_white")
    if f"{gallery}_white" not in os.listdir():
        os.mkdir(f"{gallery}_white")

    # 删除分离文件夹内的所有内容，以便重新生成
    for filname in os.listdir(f"{gallery}_not_white"):
        os.remove(f"{gallery}_not_white/{filname}")
    for filname in os.listdir(f"{gallery}_white"):
        os.remove(f"{gallery}_white/{filname}")


    count = 1
    total = len(os.listdir(gallery))
    for filename in os.listdir(gallery):
        print(f"{count}/{total}") #输出当前进度
        img = cv2.imread(f"{gallery}/{filename}", cv2.IMREAD_GRAYSCALE) # 读入图片为黑白模式
        percent = get_white_percent(img)
        cur_dir = f"{gallery}/{filename}"
        dst_dir = f"{gallery}_not_white/{filename}"
        if percent > .5: #若白色像素比例大于阈值，存入white文件夹
            dst_dir = f"{gallery}_white/{filename}"
        shutil.copy2(cur_dir,dst_dir)
        count+=1

def collect_ethics():
    all = []
    count = 1
    total = len(os.listdir(f"{gallery}_white"))
    for filename in os.listdir(f"{gallery}_white"):
        print(f"{count}/{total}")
        img = cv2.imread(f"{gallery}_white/{filename}", cv2.IMREAD_GRAYSCALE)
        all.append(get_white_percent(img))
        count+=1
    all = numpy.array(all)
    print(all.max(),all.min())
# collect_ethics()
start()
