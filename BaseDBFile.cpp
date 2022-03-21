//
// Created by adisuper on 3/16/22.
//

#include <iostream>
#include <fstream>
#include "BaseDBFile.h"

int BaseDBFile::writeDBMetaData(const char *f_path, fType type) {
    fstream metaFile;
    string metaPath = BaseDBFile::getMetaFileName(f_path);
    cout << metaPath.c_str() << endl;
    metaFile.open(metaPath, ios::out);
    metaFile << type << endl;
    metaFile.close();
    return 1;

}

string BaseDBFile::getMetaFileName(const char *f_path) {
    string metaPathString(f_path);
    size_t loc = metaPathString.find(".bin");
    metaPathString.replace(loc, 4, ".db-meta");
    return metaPathString;
}