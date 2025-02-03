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
        throw new exception("Wrong number of processes");
    }

    MPI_Status st;

    int buf = 0;

    // Создаём вектор, в котором будут соответствовать функции для конкретного процесса (со сдвигом на 2 (0 процесс и менеджер)) и порядковый номер функции
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
    cout <<functions_to_be_shared.size()<<"\n\n";
    // 0 процесс отвечает за генерацию чисел, их отправку в менеджер для начала работы конвейера и рассылку сигналов об окончании работы
    if (rank == 0) {
        int finished_reductions = 0;
        vector<int> numbers_to_be_passed = start_function(numbers_to_be_generated);
        // Первичная рассылка сгенерированных чисел
        for (int i = 0; i < numbers_to_be_generated; i++) {
            buf = numbers_to_be_passed[i];
            MPI_Send(&buf, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
            cout << "Process 0: Sent number " << buf;
        }
        // Проверка на количество чисел, прошедших через конвейер
        while (finished_reductions < numbers_to_be_passed.size()) {
            MPI_Recv(&buf, 1, MPI_INT, 1, MPI_ANY_TAG, MPI_COMM_WORLD, &st);
            finished_reductions++;
        }
        // Рассылка всем процессам сигнала об окончании работы
        MPI_Send(&buf, 1, MPI_INT, 1, functions_and_amounts.size() + 2, MPI_COMM_WORLD);
        for (int i = 0; i < functions_to_be_shared.size(); i++) {
            MPI_Send(&buf, 1, MPI_INT, i+2, get<1>(functions_to_be_shared[i]), MPI_COMM_WORLD);
        }
    }
    // Первый процесс - процесс менеджер
    if (rank == 1) {
        map<int, queue<int>> available_processes;
        // Заполняем словарь списками номеров процессов, соответствующих функциям
        for (int i = 0; i < functions_and_amounts.size(); i++) {
            available_processes[i] = queue<int>();
            for (int j = 0; j < functions_to_be_shared.size(); j++) {
                if (get<1>(functions_to_be_shared[j]) == i) {
                    available_processes[i].push(j + 2);
                }
            }
        }
        available_processes[functions_and_amounts.size()].push(size - 1);
        MPI_Request request;
        int message_received = 0;
        // Создаём очередь чисел для отправки
        deque<tuple<int, int>> queue;

        // Главный цикл
        while (true) {
            // Неблокирующий прием сообщения
            MPI_Irecv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);

            // Проверяем, пришло ли сообщение
            MPI_Test(&request, &message_received, &st);

            if (message_received > 0)
            {
                // Принимаем сигнал об окончании работы и выходим из цикла
                if (st.MPI_TAG == functions_and_amounts.size() + 2) {
                    break;
                }
                // Если мы получили число не из 0 процесса, добавляем процесс в список доступных
                if (st.MPI_SOURCE != 0)
                    available_processes[st.MPI_TAG - 1].push(st.MPI_SOURCE);
                // Добавляем комбинацию числа и номера следующей функции в очередь
                queue.push_back(tuple<int, int>(buf, st.MPI_TAG));
            }
            // Закрываем запрос
            else {
                MPI_Cancel(&request);
                MPI_Request_free(&request);
            }
            if(queue.size()!=0)
            {
                //Достаём первую пару для отправки из очереди
                tuple<int, int> a = queue.front();
                queue.pop_front();

                // Если нет доступных процессов у функции, возвращаем пару в конец очереди
                if (available_processes[get<1>(a)].size() == 0) {
                    // В целях выполнения условия о сохранении порядка подачи чисел в конвейер, числа полученные на момент начала ставим в начало очереди
                    if (get<1>(a) == 0) {
                        queue.push_front(a);
                    }
                    queue.push_back(a);
                }
                // Иначе отправляем свободному процессу соответствующей функции
                else {
                    buf = get<0>(a);
                    // Если получили из последнего процесса, посылаем сигнал 0 процессу о том что одно из чисел завершило путь 
                    if (st.MPI_TAG == functions_and_amounts.size() + 1) {
                        MPI_Send(&buf, 1, MPI_INT, 0, get<1>(a), MPI_COMM_WORLD);
                        continue;
                    }
                    MPI_Send(&buf, 1, MPI_INT, available_processes[get<1>(a)].front(), get<1>(a), MPI_COMM_WORLD);
                    available_processes[get<1>(a)].pop();
                }
            }
        }
    }
    // Последний процесс всегда отвечает за редукцию
    if (rank == size - 1) {
        double result;
        bool initialized = false;
        //Основной цикл
        while (true)
        {
            //Получаем число
            MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, functions_and_amounts.size(), MPI_COMM_WORLD, &st);
            // Если оно пришло из 0 процесса - это сигнал об окончании работы, выходим из цикла
            if (st.MPI_SOURCE == 0) {
                break;
            }
            else
            {
                // Если у нас нет ни одного числа в результате, мы берём за результат поступившее
                if (!initialized)
                {
                    result = buf;
                    initialized = true;
                    MPI_Send(&buf, 1, MPI_INT, 1, functions_and_amounts.size() + 1, MPI_COMM_WORLD);
                }
                // Если же есть, мы производим редукцию значения и поступившего числа
                else
                {
                    result = reduction_function(result, buf);
                    MPI_Send(&buf, 1, MPI_INT, 1, functions_and_amounts.size() + 1, MPI_COMM_WORLD);
                }
            }
        }
        // В консоль выводим результат
        if (initialized)
        {
            cout << "Outcome: " << result;
        }

    }
    // Все остальные процессы действуют одинаково
    if(rank != size - 1 && rank != 0 && rank!=1) {
        function<int(int)> function = get<0>(functions_to_be_shared[rank - 2]);
        int func_number = get<1>(functions_to_be_shared[rank - 2]);
        while (true)
        {
            // Получаем на вход число
            MPI_Recv(&buf, 1, MPI_INT, MPI_ANY_SOURCE, func_number, MPI_COMM_WORLD, &st);
            // Если источник - 0 процесс то это сигнал об окончании работы, выходим из цикла
            if (st.MPI_SOURCE == 0) {
                break;
            }
            // Иначе производим вычисление и посылаем дальше
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
    functions_and_amounts[0] = { f1, 1 };
    functions_and_amounts[1] = { f2, 1 };
    try
    {
        Conveyor(3, starting_func, functions_and_amounts, reduction);
    }
    catch (exception ex) {
        cout << ex.what();
    }
    return 0;
}