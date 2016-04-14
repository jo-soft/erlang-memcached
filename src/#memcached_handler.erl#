-module(memcached_handler). 
-export([start/0, handle/1]). 

-define(DATABASENAME, memcached_table). 

-include("datastructures.hrl").

start() ->
    ets:new(?DATABASENAME, [named_table, set, public, {keypos, #data_item.key}]).

handle(Request) ->
	case memcached_handler_server:handle_call(parse, self(), {?DATABASENAME, Request}) of 
	    {noReply, {ok, Result}} ->
		{ok,noReply} ;
	    {reply, {ok, Result}}  ->
		{ok, Result};
	    {reply, {error, Replay}} ->
		error_logger:error_msg(Replay),
		{ok, Replay}
	end.
