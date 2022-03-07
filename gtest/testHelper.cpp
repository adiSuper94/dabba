//
// Created by adisuper on 3/7/22.
//

#include <iostream>
#include <fstream>
#include "testHelper.h"
#include "../DBFile.h"
#include "../test.h"

void *producer (void *arg) {

    Pipe &myPipe = (Pipe &) arg;

    Record temp;
    int counter = 0;

    DBFile dbfile;

    dbfile.Open ("/home/adisuper/Courses/DBI/Project/tpch-dbgen/uf/nation.bin");
    dbfile.MoveFirst ();

    while (dbfile.GetNext (temp) == 1) {
        counter += 1;
        if (counter%100000 == 0) {
            cerr << " producer: " << counter << endl;
        }
        myPipe.Insert (&temp);
    }

    dbfile.Close ();
    myPipe.ShutDown ();

    cout << " producer: inserted " << counter << " recs into the pipe\n";
    return nullptr;
}

tpmms_args get_tpmms_args() {
    Pipe in(64);
    Pipe out(64);
    auto *schema = new Schema(catalog_path, "nation");
    OrderMaker om(schema);
    tpmms_args args(in, out, om, 16, 0);
    return args;
}

bool isFileOnFS(const std::string& name) {
    ifstream f(name.c_str());
    return f.good();
}
