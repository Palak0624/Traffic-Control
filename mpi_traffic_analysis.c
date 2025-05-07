#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define MAX_LINE_LENGTH 100
#define MAX_TRAFFIC_LIGHTS 1000
#define TOP_N 2
#define DEFAULT_FILENAME "traffic_data.txt"

typedef struct {
    char timestamp[20];
    char traffic_light[10];
    int count;
} TrafficRecord;

typedef struct {
    char hour[20];  // Format: "YYYY-MM-DD HH"
    char traffic_light[10];
    int count;
} HourlyStats;

void extract_hour(const char* timestamp, char* hour) {
    strncpy(hour, timestamp, 16);
    hour[16] = '\0';
    hour[13] = ' ';  // Replace ':' with space
}

int compare_stats(const void *a, const void *b) {
    const HourlyStats *sa = (const HourlyStats *)a;
    const HourlyStats *sb = (const HourlyStats *)b;
    
    int hour_cmp = strcmp(sa->hour, sb->hour);
    if (hour_cmp != 0) return hour_cmp;
    return sb->count - sa->count;
}

void process_data(TrafficRecord *records, int num_records, HourlyStats **results, int *num_results) {
    HourlyStats *temp_stats = malloc(num_records * sizeof(HourlyStats));
    if (!temp_stats) {
        perror("Failed to allocate memory for temp_stats");
        *results = NULL;
        *num_results = 0;
        return;
    }
    
    int stats_count = 0;
    
    for (int i = 0; i < num_records; i++) {
        char hour[20];
        extract_hour(records[i].timestamp, hour);
        
        int found = 0;
        for (int j = 0; j < stats_count; j++) {
            if (strcmp(temp_stats[j].hour, hour) == 0 && 
                strcmp(temp_stats[j].traffic_light, records[i].traffic_light) == 0) {
                temp_stats[j].count += records[i].count;
                found = 1;
                break;
            }
        }
        
        if (!found) {
            strncpy(temp_stats[stats_count].hour, hour, sizeof(temp_stats[0].hour)-1);
            strncpy(temp_stats[stats_count].traffic_light, records[i].traffic_light, 
                   sizeof(temp_stats[0].traffic_light)-1);
            temp_stats[stats_count].hour[sizeof(temp_stats[0].hour)-1] = '\0';
            temp_stats[stats_count].traffic_light[sizeof(temp_stats[0].traffic_light)-1] = '\0';
            temp_stats[stats_count].count = records[i].count;
            stats_count++;
        }
    }
    
    qsort(temp_stats, stats_count, sizeof(HourlyStats), compare_stats);
    
    *results = malloc(stats_count * sizeof(HourlyStats));
    if (!*results) {
        perror("Failed to allocate memory for results");
        free(temp_stats);
        *num_results = 0;
        return;
    }
    
    *num_results = 0;
    
    if (stats_count == 0) {
        free(temp_stats);
        return;
    }
    
    char current_hour[20] = "";
    int count_in_hour = 0;
    
    for (int i = 0; i < stats_count; i++) {
        if (strcmp(current_hour, temp_stats[i].hour) != 0) {
            strncpy(current_hour, temp_stats[i].hour, sizeof(current_hour)-1);
            current_hour[sizeof(current_hour)-1] = '\0';
            count_in_hour = 0;
        }
        
        if (count_in_hour < TOP_N) {
            (*results)[*num_results] = temp_stats[i];
            (*num_results)++;
            count_in_hour++;
        }
    }
    
    free(temp_stats);
}

int read_data_from_file(const char *filename, TrafficRecord **records) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return -1;
    }
    
    *records = malloc(MAX_TRAFFIC_LIGHTS * sizeof(TrafficRecord));
    if (!*records) {
        perror("Failed to allocate memory for records");
        fclose(file);
        return -1;
    }
    
    int count = 0;
    char line[MAX_LINE_LENGTH];
    
    while (fgets(line, sizeof(line), file) && count < MAX_TRAFFIC_LIGHTS) {
        // Skip empty lines or comments
        if (line[0] == '\n' || line[0] == '#') continue;
        
        // Remove newline character if present
        line[strcspn(line, "\n")] = '\0';
        
        char* parts[4];  // We'll split into 4 parts (date, time, TL, count)
        char* token = strtok(line, " ");
        int i = 0;
        
        // Split the line into parts
        while (token != NULL && i < 4) {
            parts[i++] = token;
            token = strtok(NULL, " ");
        }
        
        if (i == 4) {
            // Combine date and time for timestamp
            snprintf((*records)[count].timestamp, sizeof((*records)[0].timestamp), 
                    "%s %s", parts[0], parts[1]);
            
            // Copy traffic light ID
            strncpy((*records)[count].traffic_light, parts[2], 
                   sizeof((*records)[0].traffic_light)-1);
            (*records)[count].traffic_light[sizeof((*records)[0].traffic_light)-1] = '\0';
            
            // Parse vehicle count
            char* endptr;
            (*records)[count].count = strtol(parts[3], &endptr, 10);
            if (endptr == parts[3]) {
                printf("Warning: Could not parse count in line: %s\n", line);
                continue;
            }
            
            count++;
        } else {
            printf("Warning: Could not parse line (expected 4 parts, got %d): %s\n", i, line);
        }
    }
    
    fclose(file);
    return count;
}

int main(int argc, char *argv[]) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    if (size < 2) {
        if (rank == 0) {
            fprintf(stderr, "This program requires at least 2 processes (1 master, 1 slave)\n");
        }
        MPI_Finalize();
        return 1;
    }
    
    if (rank == 0) { // Master process
        TrafficRecord *records;
        int num_records;
        const char *filename = (argc > 1) ? argv[1] : DEFAULT_FILENAME;
        
        num_records = read_data_from_file(filename, &records);
        if (num_records < 0) {
            MPI_Finalize();
            return 1;
        }
        
        printf("Processing data from %s (%d records)\n", filename, num_records);
        
        // Debug: Print first few records
        for (int i = 0; i < (num_records < 3 ? num_records : 3); i++) {
            printf("Sample record %d: %s %s %d\n", i, records[i].timestamp, 
                   records[i].traffic_light, records[i].count);
        }
        
        int records_per_slave = num_records / (size - 1);
        int remainder = num_records % (size - 1);
        
        int offset = 0;
        for (int dest = 1; dest < size; dest++) {
            int count = records_per_slave + (dest <= remainder ? 1 : 0);
            
            MPI_Send(&count, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
            MPI_Send(&records[offset], count * sizeof(TrafficRecord), MPI_BYTE, 
                    dest, 0, MPI_COMM_WORLD);
            
            offset += count;
        }
        
        HourlyStats *all_results = NULL;
        int total_results = 0;
        
        for (int src = 1; src < size; src++) {
            int slave_results;
            MPI_Recv(&slave_results, 1, MPI_INT, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            HourlyStats *slave_data = malloc(slave_results * sizeof(HourlyStats));
            if (!slave_data) {
                perror("Failed to allocate memory for slave_data");
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            
            MPI_Recv(slave_data, slave_results * sizeof(HourlyStats), MPI_BYTE, 
                    src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            HourlyStats *temp = realloc(all_results, (total_results + slave_results) * sizeof(HourlyStats));
            if (!temp) {
                perror("Failed to reallocate memory for all_results");
                free(slave_data);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            all_results = temp;
            
            memcpy(&all_results[total_results], slave_data, slave_results * sizeof(HourlyStats));
            total_results += slave_results;
            
            free(slave_data);
        }
        
        HourlyStats *final_results = NULL;
        int num_final_results = 0;
        
        if (total_results > 0) {
            qsort(all_results, total_results, sizeof(HourlyStats), compare_stats);
            
            final_results = malloc(total_results * sizeof(HourlyStats));
            if (!final_results) {
                perror("Failed to allocate memory for final_results");
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            
            char current_hour[20] = "";
            int count_in_hour = 0;
            
            for (int i = 0; i < total_results; i++) {
                if (strcmp(current_hour, all_results[i].hour) != 0) {
                    strncpy(current_hour, all_results[i].hour, sizeof(current_hour)-1);
                    current_hour[sizeof(current_hour)-1] = '\0';
                    count_in_hour = 0;
                }
                
                if (count_in_hour < TOP_N) {
                    final_results[num_final_results] = all_results[i];
                    num_final_results++;
                    count_in_hour++;
                }
            }
        }
        
        printf("\nTop %d congested traffic lights by hour:\n", TOP_N);
        if (num_final_results > 0) {
            char current_hour[20] = "";
            int first_in_hour = 1;
            
            for (int i = 0; i < num_final_results; i++) {
                if (strcmp(current_hour, final_results[i].hour) != 0) {
                    if (!first_in_hour) printf("\n");
                    printf("For hour %s:\n", final_results[i].hour);
                    strncpy(current_hour, final_results[i].hour, sizeof(current_hour)-1);
                    current_hour[sizeof(current_hour)-1] = '\0';
                    first_in_hour = 0;
                }
                printf("    %s: %d vehicles\n", final_results[i].traffic_light, final_results[i].count);
            }
        } else {
            printf("No results to display.\n");
        }
        
        free(records);
        free(all_results);
        free(final_results);
    } else { // Slave processes
        int count;
        MPI_Recv(&count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        TrafficRecord *records = malloc(count * sizeof(TrafficRecord));
        if (!records) {
            perror("Failed to allocate memory for records in slave");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        MPI_Recv(records, count * sizeof(TrafficRecord), MPI_BYTE, 
                0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        HourlyStats *results;
        int num_results;
        process_data(records, count, &results, &num_results);
        
        MPI_Send(&num_results, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(results, num_results * sizeof(HourlyStats), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
        
        free(records);
        free(results);
    }
    
    MPI_Finalize();
    return 0;
}
