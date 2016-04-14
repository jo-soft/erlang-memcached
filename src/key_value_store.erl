.-module(key_Value_store). 
-export([start/0]). 
%% call: params, data
-export([set/2, add/2, replace/2, append/2, prepend/2, cas/2]). 
-export([get/1,gets/1]). 


start() ->
    key_value_store_server:start_link(). 

%% setter

set(Param, Payload) ->
    {reply, {ok, Result}}  = key_value_store_server:handle_call(set, self(), {Param, Payload}). 

add(Param, Payload) ->
    {reply, {ok, Result}}  = key_value_store_server:handle_call(add, self(), {Param, Payload}). 

replace(Param, Payload) ->
    {reply, {ok, Result}}  = key_value_store_server:handle_call(replace, self(), {Param, Payload}). 

append(Param, Payload) ->
    {reply, {ok, Result}}  = key_value_store_server:handle_call(append, self(), {Param, Payload}). 

prepend(Param, Payload) ->
    {reply, {ok, Result}}  = key_value_store_server:handle_call(prepend, self(), {Param, Payload}). 

cas(Param, Payload) ->
    {reply, {ok, Result}}  = key_value_store_server:handle_call(cas, self(), {Param, Payload}). 


%% getter
get(Key) ->
    {reply, {ok, Result}}  = key_value_store_server:handle_call(get, self(), Key). 

gets(Keys) ->
    {reply, {ok, Result}}  = key_value_store_server:handle_call(gets, self(), Keys). 
