#include "cli.h"

uint64_t key;
string ckey;
string value;


void show_cli() {
    cout << "enter interactive mode: " << endl;
    cout << "input form: command key value" << endl;
    cout << "command: put / get / set / del / count / exit" << endl;
    cout << "key: integer" << endl;
    cout << "value: string in 15B" << endl;
    cout << endl;
    string command;
    int ret = 0;

    // loop until "exit" command is typed
    while (ret != 1) {
        cout << "rewo_cli > ";
        cin >> command;
        ret = parse_cli(command);
        cout << endl;
    }
}

int parse_cli(string command) {
    int ret;
    char *value_get = (char *)malloc(VALUE_SIZE);

    if (strcmp(command.c_str(), (char *)"put") == 0) {
#if KEY_TYPE == 1
        cin >> key >> value;
        ret = rewo_insert(key, (char *)value.c_str());
        if (ret == 0) {
            cout << "key " << key << " insert ok! " << endl;
            return 0;
        }
#else
        cin >> ckey >> value;
        ret = rewo_insert((char *)ckey.c_str(), (char *)value.c_str());
        if (ret == 0) {
            cout << "key " << ckey << " insert ok! " << endl;
            return 0;
        }
#endif
    }
    else if (strcmp(command.c_str(), (char *)"get") == 0) {
#if KEY_TYPE == 1
        cin >> key;
        ret = rewo_search(key, value_get);
        if (ret == 0) {
            cout << "value: " << value_get << endl;
        }
        else {
            cout << "key " << key << " does not exist! " << endl;
        }
        return 0;
#else
        cin >> ckey;
        ret = rewo_search((char *)ckey.c_str(), value_get);
        if (ret == 0) {
            cout << "value: " << value_get << endl;
        }
        else {
            cout << "key " << ckey << " does not exist! " << endl;
        }
        return 0;
#endif
    }
    else if (strcmp(command.c_str(), (char *)"set") == 0) {
#if KEY_TYPE == 1
        cin >> key >> value;
        ret = rewo_update(key, (char *)value.c_str());
        if (ret == 0) {
            cout << "key " << key << " update ok! " << endl;
        }
        else {
            cout << "key " << key << " does not exist! " << endl;
        }
        return 0;
#else
        cin >> ckey >> value;
        ret = rewo_update((char *)ckey.c_str(), (char *)value.c_str());
        if (ret == 0) {
            cout << "key " << ckey << " update ok! " << endl;
        }
        else {
            cout << "key " << ckey << " does not exist! " << endl;
        }
        return 0;
#endif
    }
    else if (strcmp(command.c_str(), (char *)"del") == 0) {
#if KEY_TYPE == 1
        cin >> key;
        ret = rewo_delete(key);
        if (ret == 0) {
            cout << "key " << key << " delete ok! " << endl;
        }
        else {
            cout << "key " << key << " does not exist! " << endl;
        }
        return 0;
#else
        cin >> ckey;
        ret = rewo_delete((char *)ckey.c_str());
        if (ret == 0) {
            cout << "key " << ckey << " delete ok! " << endl;
        }
        else {
            cout << "key " << ckey << " does not exist! " << endl;
        }
        return 0;
#endif
    }
    else if (strcmp(command.c_str(), (char *)"count") == 0) {
        cout << "total kv number: " << rewo_get_kv_num() << endl;
        return 0;
    }
    else if (strcmp(command.c_str(), (char *)"exit") == 0) {
        cout << "thanks for using and see you next time!" << endl;
        return 1;
    }

    cout << "operation failed! please restart the hash table!" << endl;
    return 1;
}


