#include <iostream>
#include <string.h>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <queue>
#include <sstream>
#include <libgen.h>
#include <math.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

using namespace std;

#define MILLION 1000000
static int create_file = 0;
static int total_dir = 0;
static string ori_file = "/mnt/myfs/example.txt";

void clean_dir(string root)
{
	cout<<"Begin to clean dir"<<endl;
	string cmd = "rm -rf ";
	cmd.append(root);
	cmd.append("/*");
	char tmp[32];
	memset(tmp, 0, 32);
	strcpy(tmp, cmd.c_str());
    system(tmp);
    cout<<"Clean dir done"<<endl;
}

void makedir_recursive(int depth, int width, string root)
{
    string path = root;
    string name = "";
    char tmp[32];
    memset(tmp, 0, 32);
    if(depth <= 2)
    {
        for(int i = 0; i < width; i++)
        {
            stringstream ss;
            ss<<i;
            ss>>name;
            path.append("/");
            path.append(name);
            total_dir++;
	    strcpy(tmp, path.c_str());
            int ret = mkdir(tmp, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
            if (ret != 0) {
                cout<<"path = "<<path<<" not create"<<endl;
				return;
			}
            strcpy(tmp, path.c_str());
			path = dirname(tmp);
        }
        return;
    }
    for(int i = 0; i < width; i++)
    {
        stringstream ss;
        ss<<i;
        ss>>name;
        path.append("/");
        path.append(name);
        total_dir++;
	strcpy(tmp, path.c_str());
        int ret = mkdir(tmp, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        if (ret != 0) {
            cout<<"path = "<<path<<" not create"<<endl;
            return;
        }
        depth = depth - 1;
        makedir_recursive(depth, width, path);
        depth = depth + 1;
        strcpy(tmp, path.c_str());
        path = dirname(tmp);
    }
}

void makedir_non_recursive(int depth, int width, string root)
{
    queue<string> q;
    string str;
    int step = 0;
    int ret = 0;
    int j;
	char tmp[32];
	memset(tmp, 0, 32);
	string name = "";
	string path = "";
	string cmd = "";

    q.push(root);
    int sum = (pow(width, depth) - 1) / (width - 1);
    total_dir = sum;
    sum--;

    step++;
    while(!q.empty() && sum > 0)
    {
        path = q.front();
        q.pop();
        for(int i = 0; i < width; i++)
        {
            stringstream ss;
            ss<<i;
            ss>>name;
            path.append("/");
            path.append(name);
	    strcpy(tmp, path.c_str());
	    ret = mkdir(tmp, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
            //ret = 0;
	    if (create_file)
            {
		cmd = "cp /example.txt ";
		cmd.append(path);
		char cmd_t[60];
		memset(cmd_t, 0, 32);
		strcpy(cmd_t, cmd.c_str());
		system(cmd_t);
		cout<<"exec cmd = "<<cmd_t<<endl;
            }
			if (ret != 0) {
                cout<<"path = "<<path<<" not create"<<endl;
				return;
			}
			sum--;
			q.push(path);
			step++;
			strcpy(tmp, path.c_str());
			path = dirname(tmp);
        }
    }
}

int main()
{
    create_file = 0;
     struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);
    makedir_non_recursive(3, 128,"/mnt/lustre/pre_alloc");
    clock_gettime(CLOCK_MONOTONIC, &end);
    long exec_time;
    exec_time = (end.tv_sec - start.tv_sec) * 1000  + (end.tv_nsec - start.tv_nsec) / MILLION;
    printf("exec time = %ld (m seconds)\n", exec_time);
    cout<<"Total dir (non-recursive) = "<<total_dir<<endl;
    return 0;
}
