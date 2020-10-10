#pragma once

#include <string>
#include <iostream>
#include "api.h"

using namespace std;

/* enter interactive mode */
void show_cli();

/* require more parameters or stop based on parsing the command */
int parse_cli(string command);
