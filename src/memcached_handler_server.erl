-compile(debug_info).
%%% @author Johannes <j_schn14@(none)>
%%% @copyright (C) 2015, Johannes
%%% @doc
%%%
%%% @end
%%% Created : 27 Jun 2015 by Johannes <j_schn14@(none)>
%%%-------------------------------------------------------------------
-module(memcached_handler_server).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include("datastructures.hrl").

-define(SERVER, ?MODULE).

-define(STORED,"STORED\r\n").
-define(NOT_STORED,"NOT_STORED\r\n").
-define(DELETED, "DELETED\r\n"). 
-define(NOT_FOUND, "NOT_FOUND\r\n"). 

% blank at the end is important!
-define(SERVER_ERROR, "SERVER_ERROR "). 

% Time switch between exp time in seconds and exp time as  date.
-define(SECONDS30DAYS, 60*60*24*30). 

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    key_Value_store:start(),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(parse, From, Data) ->
    try
	From ! parse(Data)
    catch 
	Exception:Reason ->
	    From ! {reply, server_error(Exception, Reason)}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

split_at_newLine([]) ->
    [];
split_at_newLine(Val) ->
    Val1 = re:replace(Val, "\r", "", [{return, list}]),
    Foo  = lists:splitwith(fun(V)  ->
				   V == 10 end,
			   Val1),
    Foo.

remove_RN(Val) ->
    re:replace(Val, "\r|\n", "", [global, {return, list}]).     


server_error(Exception, Reason) ->
    ExceptionString = atom_to_list(Exception),
    ReasonString = atom_to_list(Reason),
    {error, ?SERVER_ERROR ++ ExceptionString ++ ":" ++ ReasonString ++ "\r\n"} .

parse({TableName, BinaryData}) ->
    Data = binary_to_list(BinaryData),
    Tokens = string:tokens(Data, " "),
    [CommandString | Params] = Tokens,
    Command = list_to_atom(CommandString),
    Result = handle_command(Command, Params, TableName),
    {reply, Result}. 


expired(Data) when Data#data_item.expTime == 0 ->
    % expire date == 0 -> do not expire.
    false;
expired(Data) ->
    % when data got inserted, it's always converted into absolute expire dates.
    ExperationTime = Data#data_item.expTime,
    ExperationTime < os:system_time(). 

lookup(TableName, Key) ->
    case ets:lookup(TableName, Key) of
	[] -> 
	    {not_found, []};
	[Data] -> case expired(Data) of 
			   true ->
			       {expired, []};
			   false ->
			       {ok, Data}
		       end
	end.

databaseCall(Key, Params, Data, Fn) ->
    CleanKey = clean_key(Key),
    CleanData = clean_data(CleanKey, Params, Data),
    Fn(CleanData). 

insert(TableName, Key, Params, Data, replace) ->
    Fn = fun(D)  -> ets:insert(TableName, D) end,
    databaseCall(Key, Params, Data, Fn);
insert(TableName, Key, Params, Data, add) ->
    Fn = fun(D)  -> ets:insert_new(TableName, D) end,
    databaseCall(Key, Params, Data, Fn ).

clean_key(Key) ->
    remove_RN(Key).

clean_data(Key, Params, Data) ->
    NewData = build_data(Data), 
    [Flags, ExpTime, Bytes | MaybeCas ] = Params, 
    Cas =  create_cas(MaybeCas),
    case string:to_integer(ExpTime) of 
	{ExpTimeInt, []} ->
	    RealExpTime = calculate_real_exp_time(ExpTimeInt)
    end,
    #data_item{key=Key,flags=Flags,expTime=RealExpTime, bytes=Bytes, cas=Cas,value=NewData}. 

create_cas([Cas]) ->
    Cas;
create_cas([]) ->
    erlang:unique_integer([positive]).

build_data(Data) ->
    NewData = remove_RN(Data),
    case string:to_integer(NewData) of 
	{Integer, []} ->
	    Integer;
	{_ErrorOrInteger, _Rest} ->
	    NewData
    end.

calculate_real_exp_time(ExpValue) when ExpValue == 0 ->
    % does not expire
    0;
calculate_real_exp_time(ExpValue) when ExpValue < ?SECONDS30DAYS ->
    % Expiredate given relative
    os:system_time() + ExpValue;
calculate_real_exp_time(ExpValue) ->
    % expire date given absolute
    ExpValue. 
	

ensureString(Val) ->
    case lists:is_list(Val) of 
	true ->
	    Val;
	false ->
	    integer_to_list(Val)
    end. 
	

handle_command(set, [cas | Params], _TableName) ->
    { error, {not_implemented, {set, [cas | Params]}}};

handle_command(set, Params, TableName) ->
    [Key | Data] = Params,
    Value = lists:last(Data),
    Value1 = split_at_newLine(Value),
    Flags = lists:droplast(Data),
    insert(TableName, Key, Flags, Value, add),
    {ok, ?STORED};

handle_command(add, Params, TableName) ->
    [Key | Data] = Params,
    Value = lists:last(Data),
    Flags = lists:droplast(Data),
    insert(TableName, Key, Flags, Value, replace),
    {ok, ?STORED};    
    
handle_command(replace, [Key | Data] , TableName) ->
    CleanKey = clean_key(Key),
    case lookup(TableName, CleanKey) of
        {_Reason, []} -> 
	    % this should catch reason = not_found and reason = expired.
	    {error, ?NOT_STORED};
        _Data ->
	    Value = lists:last(Data),
	    Flags = lists:droplast(Data),
	    insert(TableName, Key, Flags, Value, add),
          {ok, ?STORED}
    end;

handle_command(append, [Key | Data], TableName) ->
    CleanKey = clean_key(Key),
    case lookup(TableName, CleanKey) of
        {_Reason, []} -> 
          {error, ?NOT_STORED};
        {ok, Received} ->
	    Value = lists:last(Data),
	    Flags = lists:droplast(Data),
	    OldValue = ensureString(Received#data_item.value),
	    NewValue = ensureString(Value),
	    ExtendedValue = [lists:append(OldValue, NewValue)],
	    insert(TableName, Key, Flags, ExtendedValue, add),
	    {ok, ?STORED}  
    end;

handle_command(prepend,  [Key | Data], TableName) ->
    CleanKey = clean_key(Key),
    case lookup(TableName, CleanKey) of
        {_Reason, []} -> 
          {error, ?NOT_STORED};
	{ok, Received} ->
	    Value = lists:last(Data),
	    Flags = lists:droplast(Data),
	    OldValue = ensureString(Received#data_item.value),
	    NewValue = ensureString(Value),
	    ExtendedValue = [lists:append(NewValue, OldValue)],
	    insert(TableName, Key, Flags, ExtendedValue, add),
	  {ok, ?STORED}  
    end;


handle_command(get, Keys, TableName) ->
    CleanKeys = lists:map(fun(Key) -> clean_key(Key) end, Keys),
    LookupResults = lists:map( fun(Key) -> lookup(TableName, Key) end,  CleanKeys),
    LookupResultHits = lists:filter( fun({Reason, _Item}) ->  Reason == ok end, LookupResults),
    ReturnLines = lists:map(fun(Result) -> result_to_value_line(Result) end, LookupResultHits),
    Result = lists:concat(ReturnLines ++  ["END\r\n"]),
    {ok, Result};

handle_command(delete, Key, TableName) ->
    CleanKey = clean_key(Key),
    case ets:member(TableName, CleanKey) of 
	true ->
	    ets:delete(TableName, CleanKey),
	    {ok, ?DELETED};
	false  ->
	    {ok, ?NOT_FOUND}
    end;

handle_command(incr, [Key, Value], TableName) ->
    try 
	% need to be done here, otherwise we try to apply - (minus) on a string in decr.
	{CleanValue, _Rest}  = string:to_integer(remove_RN(Value)),
	NewValue = change_counter(Key, CleanValue, TableName),
	{ok, NewValue ++ "\r\n"}
    catch
	error:badarg -> {ok, ?NOT_FOUND}
    end;

handle_command(decr, [Key, Value],  TableName) ->
    try
	% need to be done here, otherwise we try to apply - (minus) on a string
	{CleanValue, _Rest}  = string:to_integer(remove_RN(Value)),
	NewValue = change_counter(Key, -CleanValue, TableName),
	{ok, NewValue ++ "\r\n"}
    catch
	error:badarg -> {ok, ?NOT_FOUND}
    end;

handle_command(Cmd, _Params, _Table) ->
    {error, {unkown_command, Cmd}}. 


%handle_command(gets, _Keys, _TableName) ->
% {ok }.	

change_counter(Key, Value, TableName) ->
    CleanKey = clean_key(Key),
    try
	% try to update the value in place
	ets:update_counter(TableName, CleanKey, {#data_item.value, Value})
    catch
	% update_counter fails with badarg if value is not an integer
	% solution: get the value and try to parse it as int (results in a type conversion from string to int)
	error:badarg ->
	    case lookup(TableName, CleanKey) of
		{ok, Received} ->
		    Val = Received#data_item.value,
		    case string:to_integer(Val) of 
			{Integer, []} ->
			    Flags = Received#data_item.flags,
			    insert(TableName, CleanKey, Flags, Integer + Value, add)
		    end
	    end
    end.



getValueFromRecord(LookupResult) ->
    Data = LookupResult#data_item.value,
    try
	integer_to_list(Data)
    catch
	error:badarg ->
	    Data
    end.


result_to_value_line({ok, LookupResult}) ->
    Key = LookupResult#data_item.key,
    Flags = LookupResult#data_item.flags,
    ReturnValLength = LookupResult#data_item.bytes,
    ReturnValue = getValueFromRecord(LookupResult),
    "VALUE " ++ Key  ++ " " ++ Flags ++ " " ++ ReturnValLength ++ "\r\n" ++ ReturnValue ++ "\r\n".

