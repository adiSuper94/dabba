//
// Created by adisuper on 3/7/22.
//
#pragma once

#include "../Pipe.h"
#include "../BigQ.h"

void *producer (void *arg);
tpmms_args get_tpmms_args();
bool isFileOnFS(const std::string& name);