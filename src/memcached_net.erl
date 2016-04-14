-module(memcached_net). 
-export([init/0]).

init () ->
    memcached_handler:start(),

    case gen_tcp:listen(11211, [binary, {keepalive, true}, {reuseaddr, true},  {packet, 0}, {active, true} ]) of
    {ok, Listen} ->
	    spawn(fun() ->
			  connect(Listen) end);
    {error, Reason}  ->
	    exit(Reason)
    end.

	    
	    

connect (Listen) ->
    {ok, Socket} = gen_tcp:accept(Listen),
    spawn(fun() ->
		  connect(Listen) end),
    do_receive(Socket). 

do_receive(Socket) ->
    receive
	{tcp, Socket, Request} ->
	    io:format("Request: ~p~n", [Request]),
	    case memcached_handler:handle(Request) of 
		{ok, noReply} -> ;
		    io:format("No Reply: ~p~n"),
		{ok, Result} ->
		    BinaryResult = list_to_binary(Result),	    
		    io:format("Returning: ~p~n", [BinaryResult]),
		    gen_tcp:send(Socket, BinaryResult)
	    end;
	    do_receive(Socket);
	{tcp_closed, Socket } -> 
	    io:format("Closing Socket: ~w~n", [Socket]),
	    gen_tcp:close(Socket),
	    io:format("Socket Closed: ~w~n", [Socket])
    end. 
