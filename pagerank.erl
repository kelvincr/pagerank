% Instituto Tecnológico de Costa Rica
% Bases de Datos Avanzadas
% Kelvin Jimenez Morales
% PageRank
-module(pagerank).
-compile(export_all).

repeat_func(N,Func) ->
  lists:map(Func, lists:seq(0, N-1)).

%%%%%%%%%%%%%%%%%%%%%%%%  Worker Pool %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
pool(_, _, _, Results, 0) ->
    Results;
pool(Verb, Workers, Works, Results, PendingResults) ->
    {Available_Workers, Pending_Works} = pool_process(Verb, Workers, Works, Results, PendingResults),
    {New_Workers, New_Results, New_PendingResults} = pool_receive(Verb, Available_Workers, Pending_Works, Results, PendingResults),
    pool(Verb, New_Workers, Pending_Works, New_Results, New_PendingResults).
  

pool_process(Verb, Workers, Works, Results, PendingResults) ->
   Available = PendingResults > 0 andalso queue:len(Workers) > 0 andalso length(Works) > 0,
    if Available   ->
         io:format("Workers ~p  Pending: ~p Work ~p ~n", [Workers, PendingResults, Works]),
         {{value, Worker}, New_Workers} = queue:out(Workers),
         Work2Do = hd(Works),
         New_Works = tl(Works),
         io:format("New Workers: ~p Pending Works ~p ~n", [New_Workers, New_Works]),
         {Temp_Available_Workers, Temp_Pending_Works} = pool_process(Verb, New_Workers, New_Works, Results, PendingResults),
         io:format("Sending work from ~p to ~p work: ~p ~n", [self(),Worker,Work2Do]),
         Worker ! { Verb, self(), Work2Do },
         {Temp_Available_Workers, Temp_Pending_Works};
         not Available -> {Workers, Works}              
    end.

  pool_receive(Verb, Workers, Works, Results, PendingResults) ->
  receive
      {completed, Pid, Result} ->
          New_Workers = queue:in(Pid, Workers),
          New_Results = [Result | Results],
          io:format("Completed ~p ... ~p ~n", [Pid, Result]),
          {New_Workers, New_Results, PendingResults - 1};
          { _ } -> io:format("Woops... ~n", [])
  end.
    

pool(Verb, Workers, Works) ->
    Workers_Queue = queue:from_list(Workers),
    pool(Verb, Workers_Queue, Works, [],length(Works)).

pool_task(PoolFun, Params) ->
  Verb = hd(Params),
    receive
    { Verb, Controller, Data} ->
        io:format("Working ~p ... ~p ~p ~n", [self(), Data, Params]),
      Result = PoolFun(Data, tl(Params)),
      Controller ! {completed, self(),Result},
      pool_task(PoolFun, Params);
    { Command, Controller, _} -> io:format("~p has recieved Invalid command ~w from ~p ~n",[self(), Command, Controller]);
    {kill} -> io:format("Bye Worker ~p ~n",[self()])
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%% Map Reduce %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
map_reduce(Mappers, Reducers, Chunks) ->
io:format("################### Chunks...  ###################~n ~p ~n", [Chunks]),
% Map - Reduce
io:format("################### Mappers Working... ~p ###################~n", [Mappers]),
MappedResults = pool(map, Mappers, Chunks),
io:format("+++++++++++ Mapped... +++++++++++  ~n ~p ~n", [MappedResults]),
io:format("################### Reducers Working... ~p ###################~n", [Reducers]),
Results = pool(reduce, Reducers, MappedResults),
io:format("+++++++++++ Reduced... +++++++++++  ~n ~p ~n", [Results]),
 Results.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%  Workers %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_workers(Quantity, File, Processor,Func, Params) ->
    Workers = repeat_func(Quantity,
      fun(_) ->
        spawn(File, Processor,
          [Func, Params])
      end),
  io:format("Workers ~w are started~n",
    [Workers]), Workers.

kill_workers(Workers) ->
    lists:foreach( fun(Worker) ->
                Worker ! {kill}
            end, Workers).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_vector() ->
    [1,2,3,4,5,6].

get_matrix() ->
    [[3,6],[2,5],[1],[],[5,6],[2,3,4]].

%% [i,j,w]
map_multiplication(Data, Vector) ->
      io:format("Mapping: Data ~p ~n", [Data]),
    lists:map(fun([I, W, R]) -> [I, W * R] end, Data).

reduce_multiplication(Data, _) ->
    Key = hd(hd(Data)),
    [Key, lists:sum(
    lists:map(fun([_, Value]) -> 
        Value    
        end, Data)
    )].

merge_multiplication(Data, VectorLength) ->
    EmptyList = lists:seq(1, VectorLength, 1),
    io:format("EmptyList: Data ~p ~n", [EmptyList]),
    BaseDict = dict:from_list(lists:map(fun(X) -> {X,0} end, EmptyList)),
    List = lists:map(fun([Key,Value]) -> {Key,Value} end, Data),
    io:format("List: Data ~p ~n", [List]),
    Dict = dict:from_list(List),
    SortedList = lists:keysort(1, dict:to_list(dict:merge(fun(_, V1, V2) -> V1 + V2 end, BaseDict, Dict))),
    io:format("SortedList: Data ~p ~n", [SortedList]),
    lists:map(fun({ _ , V}) -> V end, SortedList).

mult_matrix(Matrix, Vector) ->
M = create_workers(1, pagerank, pool_task, fun(Data, Params) -> map_multiplication(Data, Params) end, [map, Vector]),
R = create_workers(1, pagerank, pool_task, fun(Data, _) -> reduce_multiplication(Data, []) end, [reduce]),
Chunks = create_chunks(Matrix, Vector),
MR = map_reduce(M,R, Chunks),
  timer:sleep(2000),
kill_workers(M),
kill_workers(R),
 io:format("MR: Data ~p ~n", [MR]),
merge_multiplication(MR, length(Vector)).

%% [[Row, Weight, Columns],...,[Row, Weight, Columns]]
create_chunks(Matrix, Vector) ->
    element(1,
        lists:mapfoldl(
            fun(Row, Index) -> { process_row(Row,Index, Vector), Index + 1} 
            end, 1, Matrix)
        ).

process_row(Row, Index, Vector) ->
    Columns = length(Row),
    if Columns > 0 ->
        lists:map(fun(Column) -> 
                        [Index, 1/Columns, lists:nth(Column, Vector) ] 
                        end, Row);
       Columns == 0 -> [[Index, 0.0, 0]]
    end.    

bin_to_num(Elem) ->
    try list_to_float(Elem)
    	catch error:badarg -> list_to_integer(Elem)
    end.

readVector(FileName, EOL) ->
    {ok, Binary} = file:read_file(FileName),
    Lines = string:tokens(erlang:binary_to_list(Binary), EOL),
    [[bin_to_num(X) || X<-string:tokens(Y, ",")] || Y<-Lines],
    io:format("~s~n",lists:nth(0, Lines)).

testAll() ->
    Matrix = get_matrix(),
    Vector = get_vector(),
    mult_matrix(Matrix, Vector).

test() ->
    Matrix = get_matrix(),
    Chunks = create_chunks(Matrix, get_vector()),
    Packet = hd(Chunks),
    Map = map_multiplication(Packet, get_vector()),
    Reduce = reduce_multiplication(Map,[]),
    merge_multiplication([Reduce], length(get_vector())).