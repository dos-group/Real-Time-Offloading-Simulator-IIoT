#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char *argv[])
{
    clock_t start = clock();

    if (argc != 3)
    {
        printf("Usage: task execution_cpu_time_ms return_data_bytes\n");
        return -1;
    }

    const double execution_time = atoi(argv[1]);
    const size_t return_data_bytes = atoi(argv[2]);

    char *ptr = (char *)malloc(return_data_bytes);
    for (int i = 0; i < return_data_bytes; i++)
    {
        ptr[i] = 'a';
    }

    double elapsed;
    do
    {
        clock_t now = clock();
        elapsed = (double)(now - start) / CLOCKS_PER_SEC * 1000;
    } while (elapsed < execution_time);

    printf("%s", ptr);
    return 0;
}