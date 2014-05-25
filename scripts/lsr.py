#!/usr/bin/env python

import os

def run(folder_name="./data/images/"):
    out_file = open('./out.txt', 'wb')
    for item in os.listdir(folder_name):
        if item.endswith("jpg"):
            line = folder_name + item + '\n'
            print line
            out_file.write(line)

    out_file.close()
    
if __name__ == "__main__" :
    run()  