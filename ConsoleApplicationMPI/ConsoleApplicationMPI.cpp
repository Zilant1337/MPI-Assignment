#include <iostream>
#include <mpi.h>
#include <vector>
#include <map>
#include <functional> 
#include <tuple>
#include "ConsoleApplicationMPI.h"

using namespace std;

int func(int x) 
{
	return(10*x);
}

void master_slave1() {
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	MPI_Status st;

	int buf = 0;
	if (rank == 0)
	{
		int init_sum = 0;
		int sum = 0;
		int N;
		cout << "Enter a number of random numbers:\n";
		cin >> N;
		vector<int> numbers(N);

		for (int i = 0; i < N; i++) {

			numbers[i] = rand();
			init_sum += func(numbers[i]);
		}
		for (int i = 1; i < size; i++) {
			if (i >= N) {
				break;
			}
			buf = numbers[i - 1];

			MPI_Send(&buf, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
		}
		int next_id = size - 1;
		int closed_process_count = 0;
		while (true) {
			MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &st);
			cout << "Recieved BUF:" << buf << "\n";
			sum += buf;
			if (next_id < N)
			{
				buf = numbers[next_id];
				MPI_Send(&buf, 1, MPI_INT, st.MPI_SOURCE, 0, MPI_COMM_WORLD);
				next_id++;
			}
			else {
				buf = -1;
				MPI_Send(&buf, 1, MPI_INT, st.MPI_SOURCE, 1, MPI_COMM_WORLD);
				closed_process_count++;
				if (closed_process_count == size - 1) {
					break;
				}
			}
		}
		cout << "Initial sum: " << init_sum << "\nSum: " << sum;
	}
	else
	{
		while (true)
		{
			MPI_Recv(&buf, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
			if (st.MPI_TAG == 1) {
				break;
			}
			cout << buf << endl;
			buf = func(buf);
			MPI_Send(&buf, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
		}
	}

	MPI_Finalize();
}

void butterfly_sum_pow2() {

	int rank, size,step_to_output = 3;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	MPI_Status st;

	int num= rank+1;
	int cnt = 0;

	for (int step_dist = 1; step_dist <= size / 2;step_dist*=2) {
		int buf = num;
		if (rank % (step_dist * 2) < step_dist) {
			MPI_Send(&buf, 1, MPI_INT, rank + step_dist, 0, MPI_COMM_WORLD);
			MPI_Recv(&buf, 1, MPI_INT, rank + step_dist, 0, MPI_COMM_WORLD, &st);
		}
		else {
			MPI_Send(&buf, 1, MPI_INT, rank - step_dist, 0, MPI_COMM_WORLD);
			MPI_Recv(&buf, 1, MPI_INT, rank - step_dist, 0, MPI_COMM_WORLD, &st);
		}
		num = num + buf;
		cnt++;
		if(cnt == step_to_output)
			cout<<"Step: "<<cnt<< " Rank: " << rank << " Num: " << num << "\n";
	}
	MPI_Finalize();
}
void butterfly_sum() {

	int rank, size, step_to_output = 3;

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	MPI_Status st;

	int num = rank + 1;
	int cnt = 0;

	for (int step_dist = 1; step_dist <= size / 2; step_dist *= 2) {
		int buf = num;
		if (rank % (step_dist * 2) < step_dist) {
			MPI_Send(&buf, 1, MPI_INT, rank + step_dist, 0, MPI_COMM_WORLD);
			MPI_Recv(&buf, 1, MPI_INT, rank + step_dist, 0, MPI_COMM_WORLD, &st);
		}
		else {
			MPI_Send(&buf, 1, MPI_INT, rank - step_dist, 0, MPI_COMM_WORLD);
			MPI_Recv(&buf, 1, MPI_INT, rank - step_dist, 0, MPI_COMM_WORLD, &st);
		}
		num = num + buf;
		cnt++;
		if (cnt == step_to_output)
			cout << "Step: " << cnt << " Rank: " << rank << " Num: " << num << "\n";
	}
	MPI_Finalize();
}

float Conveyor(int numbers_to_be_generated, function<vector<int>(int)> start_function, map<function<int(int)>, int> functions_and_amounts, function<int(int,int)> reduction_function) {
	//int randNum = rand() % (max - min + 1) + min;
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	int sum_of_elems = 0;
	for (auto& n : functions_and_amounts)
		sum_of_elems += (int)n.second;

	if (sum_of_elems > size - 2) {
		throw new exception("Not enough processes");
	}

	MPI_Status st;

	int buf = 0;
	
	vector<tuple<function<int(int)>,int>> functions_to_be_shared;
	int i = 0;
	for (auto& n : functions_and_amounts)
	{
		for (int j = 0; j < n.second; j++) 
		{
			functions_to_be_shared.push_back({ n.first,i });
		}
		i++;
	}

	if (rank == 0) {
		vector<int> numbers_to_be_passed = start_function(numbers_to_be_generated);
		for (int i = 0; i < numbers_to_be_generated; i++) {
			buf = numbers_to_be_passed[i];
			MPI_Send(&buf, 1, MPI_INT, MPI_ANY_SOURCE , 0 , MPI_COMM_WORLD);
		}
		MPI_Send(&buf, 1, MPI_INT, 1, functions_and_amounts.size()+1, MPI_COMM_WORLD);
	}
	if (rank == size-1) {
		double result;
		bool initialized = false;

		while (true)
		{
			MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
			if (st.MPI_TAG == functions_and_amounts.size() + 1) {
				break;
			}
			if(st.MPI_TAG == functions_and_amounts.size())
			{
				if (!initialized)
				{
					result = buf;
					initialized = true;
				}
				else
				{
					result = reduction_function(result, buf);
				}
			}
			else {
				MPI_Send(&buf, 1, MPI_INT, st.MPI_SOURCE, st.MPI_TAG, MPI_COMM_WORLD);
			}
		}
		if (initialized)
		{
			cout << "Outcome: " << result;
		}
		
	}
	else {
		while(true)
		{
			MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
			if (st.MPI_TAG == functions_and_amounts.size() + 1) {
				break;
			}
			if(st.MPI_TAG == get<1>(functions_to_be_shared[rank - 1]))
			{
				buf = get<0>(functions_to_be_shared[rank - 1])(buf);
				MPI_Send(&buf, 1, MPI_INT, get<1>(functions_to_be_shared[rank]), 0, MPI_COMM_WORLD);
			}
			else {
				MPI_Send(&buf, 1, MPI_INT, st.MPI_SOURCE, st.MPI_TAG, MPI_COMM_WORLD);
			}
		}
		MPI_Send(&buf, 1, MPI_INT, rank + 1, 1, MPI_COMM_WORLD);
	}
}

int main(int argc, char* argv[])
{
	int errCode;

	if ((errCode = MPI_Init(&argc, &argv)) != 0)
	{
		return errCode;
	}

	

	
	//master_slave1();
	//butterfly_sum_pow2();
	
	return 0;
}