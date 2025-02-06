#include <iostream>
#include <mpi.h>
#include <vector>
#include <map>
#include <functional>
#include <tuple>
#include <queue>
#include <deque>

using namespace std;

int func(int x)
{
    return(10 * x);
}

vector<int> starting_func(int number_count) {
    vector<int> numbers;
    for (int i = 0; i < number_count; i++) {
        numbers.push_back(i);
    }
    return numbers;
}

int f1(int a) {
    return a + 1;
}

int f2(int a) {
    return a * a;
}
int f3(int a) {
    return a * 2;
}
int reduction(int res, int a) {
    return res + a;
}

float Conveyor(int numbers_to_be_generated, function<vector<int>(int)> start_function, map<int, pair<function<int(int)>, int>> functions_and_amounts, function<int(int, int)> reduction_function) {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int sum_of_elems = 0;
    for (auto& n : functions_and_amounts)
        sum_of_elems += n.second.second;

    if (sum_of_elems != size - 3) {
        MPI_Finalize();
        throw new exception("Wrong number of processes");
    }

    MPI_Status st;

    int buf = 0;

    
    vector<tuple<function<int(int)>, int>> functions_to_be_shared;
    int i = 0;
    for (auto& n : functions_and_amounts)
    {
        for (int j = 0; j < n.second.second; j++)
        {
            functions_to_be_shared.push_back({ n.second.first, i });
        }
        i++;
    }
    
    if (rank == 0) {
        int finished_reductions = 0;
        vector<int> numbers_to_be_passed = start_function(numbers_to_be_generated);
        
        for (int i = 0; i < numbers_to_be_generated; i++) {
            buf = numbers_to_be_passed[i];
            MPI_Send(&buf, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
            cout << "Process 0: Sent number " << buf<<"\n";
        }
        
        while (finished_reductions < numbers_to_be_passed.size()) {
            MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
            finished_reductions++;
        }
        
        MPI_Send(&buf, 1, MPI_INT, 1, functions_and_amounts.size() + 2, MPI_COMM_WORLD);
        for (int i = 0; i < functions_to_be_shared.size(); i++) {
            MPI_Send(&buf, 1, MPI_INT, i+2, get<1>(functions_to_be_shared[i]), MPI_COMM_WORLD);
        }
        MPI_Send(&buf, 1, MPI_INT, size - 1, functions_and_amounts.size(), MPI_COMM_WORLD);
    }
    
    if (rank == 1) {
        map<int, queue<int>> available_processes;
        
        for (int i = 0; i < functions_and_amounts.size(); i++) {
            available_processes[i] = queue<int>();
            for (int j = 0; j < functions_to_be_shared.size(); j++) {
                if (get<1>(functions_to_be_shared[j]) == i) {
                    available_processes[i].push(j + 2);
                }
            }
        }
        
        MPI_Request request;
        int message_received = 0;
        
        
        queue<tuple<int, int>>queue;
        
        while (true) {
            
            MPI_Irecv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);

            
            MPI_Test(&request, &message_received, &st);

            if (message_received > 0)
            {
                
                if (st.MPI_TAG == functions_and_amounts.size() + 2) {
                    break;
                }
                cout << "Process 1: Got number " << buf<< " to be sent to function number "<<st.MPI_TAG<<"\n";
                
                if (st.MPI_SOURCE != 0 && st.MPI_SOURCE != size-1)
                {
                    available_processes[st.MPI_TAG - 1].push(st.MPI_SOURCE);
                }
                
                
                queue.push(tuple<int, int>(buf, st.MPI_TAG));
            }
            
            else {
                MPI_Cancel(&request);
                MPI_Request_free(&request);
            }
            if(queue.size()!=0)
            {
                
                tuple<int, int> a = queue.front();
                
                queue.pop();
                cout << "Number " << get<0>(a) << " To be sent to "<<get<1>(a)<<"\n";
                
                if (get<1>(a) == functions_and_amounts.size()) {
                    buf = get<0>(a);
                    MPI_Send(&buf, 1, MPI_INT, size-1, get<1>(a), MPI_COMM_WORLD);
                    continue;
                }
                
                if (available_processes[get<1>(a)].size() == 0) {
                    queue.push(a);
                    cout << "Number " << get<0>(a) << " To be sent to " << get<1>(a)<< " Returns to the queue" << "\n";
                    continue;
                }
                
                else {
                    buf = get<0>(a);
                    int process_num = available_processes[get<1>(a)].front();
                    available_processes[get<1>(a)].pop();
                    MPI_Send(&buf, 1, MPI_INT, process_num, get<1>(a), MPI_COMM_WORLD);
                    
                }
            }
            else {
                continue;
            }
        }
    }
    
    if (rank == size - 1) {
        double result;
        bool initialized = false;
        
        while (true)
        {
            
            MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, functions_and_amounts.size(), MPI_COMM_WORLD, &st);
            
            if (st.MPI_SOURCE == 0) {
                break;
            }
            else
            {
                
                if (!initialized)
                {
                    result = buf;
                    initialized = true;
                    MPI_Send(&buf, 1, MPI_INT, 0, functions_and_amounts.size() + 1, MPI_COMM_WORLD);
                    cout << "Reduction: Got number " << buf << "\n";
                }
                
                else
                {
                    result = reduction_function(result, buf);
                    cout << "Reduction: Got number " << buf << " Total reduction: "<< result << "\n";
                    MPI_Send(&buf, 1, MPI_INT, 0, functions_and_amounts.size() + 1, MPI_COMM_WORLD);
                }
            }
        }
        
        if (initialized)
        {
            cout << "Outcome: " << result;
        }

    }
    
    if(rank != size - 1 && rank != 0 && rank!=1) {
        function<int(int)> function = get<0>(functions_to_be_shared[rank - 2]);
        int func_number = get<1>(functions_to_be_shared[rank - 2]);
        while (true)
        {
            
            MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, func_number, MPI_COMM_WORLD, &st);
            
            if (st.MPI_SOURCE == 0) {
                break;
            }
            
            buf = function(buf);
            MPI_Send(&buf, 1, MPI_INT, 1, func_number + 1, MPI_COMM_WORLD);
        }
    }
    MPI_Finalize();
    return 0;
}

int main(int argc, char* argv[])
{
    int errCode;

    if ((errCode = MPI_Init(&argc, &argv)) != 0)
    {
        return errCode;
    }
    map<int, pair<function<int(int)>, int>> functions_and_amounts;
    functions_and_amounts[0] = { f1, 3 };
    functions_and_amounts[1] = { f2, 7 };
    functions_and_amounts[2] = { f3, 5 };
    try
    {
        Conveyor(10, starting_func, functions_and_amounts, reduction);
    }
    catch (exception ex) {
        cout << ex.what();
    }
    return 0;
}