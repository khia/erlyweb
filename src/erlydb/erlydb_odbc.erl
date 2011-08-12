%% Copyright (c) 2009 Ilya Khlopotov <ilya.khlopotov (at) gmail.com>
%% Inspired by work of Yariv Sadan and Matthew Pflueger
%%     Copyright (c) 2006-2007 Yariv Sadan <yarivvv (at) gmail.com>
%%     Copyright (c) 2007 Matthew Pflueger <matthew.pflueger  (at) gmail.com>
%% Developed based on erlydb_mnesia and erlydb_mysql
%% For license information see LICENSE.txt

-module(erlydb_odbc).
-author('ilya.khlopotov@gmail.com').

-behaviour(gen_server). 

%%--------------------------------------------------------------------
%% Include files
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Import libraries
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Macro definitions
%%--------------------------------------------------------------------
%% erlc -o ../ebin -DEBUG erlydb_odbc.erl
-ifdef(DEBUG).
-define(debug(Format, Args, Fun), 
	.io:format("~p::~p::~p:~p:" ++ Format ++ "~n", 
		   [?MODULE, Fun, ?LINE, self()] ++ Args)).
-else.
-define(debug(_Format, Args, _Fun), Args).
-endif.

-define(backend, odbc).
-define(record_to_keyval(Name, Record), 
	[{F, V} || V <- tl(tuple_to_list(Record)), 
		   F <- record_info(fields, Name)]).


%%--------------------------------------------------------------------
%% API
%%
-export([
	 create_table/2,
	 select/2,
	 select_as/3,
	 update/2,
	 get_last_insert_id/2,
	 transaction/2
	 ]).

-export([
	 encode/2,
	 connect/3, %% TODO
	 get_default_pool_id/0,
	 get_metadata/1,
	 test/1
	]).

%% Server Control
-export([start/1, start/2, 
	 start_link/1, start_link/2, 
	 status/0, status/1,
	 stop/0, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, 
	 handle_info/2,
	 terminate/2, code_change/3]).

-type(proplist() :: [atom() | {atom(), term()}]).
-record(state, {odbc :: pid(),
		pool_id :: atom(),
		timeout :: integer(),
		options :: proplist()}).

create_table(Name, Opts) ->
    create_table(?MODULE, Name, Opts).
create_table(Pid, Name, Opts) ->
    case status(Pid) of
	{ok, not_running, _Pid} -> 
	    Options = Opts,
	    case start_and_connect(Options) of
		{ok, Id}  -> 
		    Ref = whereis(Id),
		    Res = handle_create_table(Name, Opts, #state{odbc = Ref}),
		    odbc:disconnect(Ref),
%%		    ?debug("RESULT ~p.",[Res], create_table),
		    Res;
		Connect_Error -> Connect_Error
	    end;
	{ok, _Pid, _Status} -> gen_server:call(Pid, {create_table, Name, Opts})
    end.

%% we cannot implement select as a gen_server because it's supposed to crash
select(Statement, Options) ->
    get_select_result(q(Statement, Options), []).
%%     select(?MODULE, Statement, Options).
%% select(Pid, Statement, Options) ->
%%     gen_server:call(Pid, {select, Statement, Options, []}).

select_as(Module, Statement, Options) ->
    get_select_result(q(Statement, Options),  [Module, false]).
%%     select_as(?MODULE, Module, Statement, Options).
%% select_as(Pid, Module, Statement, Options) ->
%%     gen_server:call(Pid, {select, Statement, Options, [Module, false]}).

sql_query(Statement) ->
    sql_query(?MODULE, Statement).
sql_query(Pid, Statement) ->
    gen_server:call(Pid, {sql_query, Statement}).

update(Statement, Options) ->
    get_update_result(q(Statement, Options), []).
    
%%     update(?MODULE, Statement, Options).
%% update(Pid, Statement, Options) ->
%%     gen_server:call(Pid, {update, Statement, Options}).

get_last_insert_id(Table, Options) ->
    get_last_insert_id(?MODULE, Table, Options).
get_last_insert_id(Pid, Table, Options) ->
    gen_server:call(Pid, {get_last_insert_id, Table, Options}).

%% We cannot implement transaction as gen_server function because it uses
%% save and insert which are in their turn gen_server functions
%% @doc Execute a group of statements in a transaction.
%%   Fun is the function that implements the transaction.
%%   Fun can contain an arbitrary sequence of calls to
%%   the odbc functions. If Fun crashes or returns
%%   or throws 'error' or {error, Err}, the transaction is automatically
%%   rolled back. 
-type(reason() :: term()).
-type(transaction_fun() ::
      fun((Driver :: string(), Options :: proplist()) -> string())).

-spec(transaction/2 :: 
      (Fun :: transaction_fun(), Options :: proplist()) -> 
	     {atomic, Result :: term()} | {aborted, reason()}).
transaction(Fun, Options) ->
    ?debug("OPTIONS: ~p (~p)",[Options, get_pool_id(Options)],transaction),
    try Fun() of
	error = Err -> 
	    commit(get_pool_id(Options), rollback),
	    throw(Err);
	{error, _} = Err -> 
	    commit(get_pool_id(Options), rollback),
	    throw(Err);
	{'EXIT', _} = Err -> 
	    commit(get_pool_id(Options), rollback),
	    throw(Err);
	Other -> 
	    Res = commit(get_pool_id(Options), commit),
	    ?debug("ATOMIC: ~p (~p)",[Other, Res],transaction),
	    {atomic, Other}
    catch _:Reason ->
		      commit(get_pool_id(Options), rollback),
		      throw(Reason)
    end.

commit(PoolId, Operation) ->
    gen_server:call(?MODULE, {commit, PoolId, Operation}).

%% 	{atomic, Result} ->
%% 	    ?debug("ATOMIC: ~p",[Result],transaction),
%% 	    odbc:commit(get_pool_id(Options), commit),
%% 	    {atomic, Result};
%% 	{ok, Result} ->
%% 	    ?debug("OK: ~p",[Result],transaction),
%% 	    odbc:commit(get_pool_id(Options), commit),
%% 	    {atomic, Result};
%% 	{error, _Reason} = Error -> 
%% 	    ?debug("ERROR : ~p",[Error],transaction),
%% 	    odbc:commit(get_pool_id(Options), rollback),
%% 	    {aborted, Error};
%% 	error -> 
%% 	    odbc:commit(get_pool_id(Options), rollback),
%% 	    {aborted, {error, unknown}};
%% 	Any -> 
%% 	    io:format("aborted ~p~n",[Any]),
%% 	    {aborted, {error, unknown}}
%%     catch _:Reason ->
%% 		    odbc:commit(get_pool_id(Options), rollback),
%% 		    {aborted, Reason}
%% 	    end.

%% This function has limitations it cannot determine which field is primary key
%% So we should use describe_table_fun option to provide all function
describe_table(Table, Options) ->
    describe_table(?MODULE, Table, Options).
describe_table(Pid, Table, Options) ->
    gen_server:call(Pid, {describe_table, Table, Options}).

-type(pool_ref() :: atom() | pid()).
-spec(start/1 :: (Options :: proplist()) -> 
	     {ok, pool_ref()} | {error, reason()}).
start(Options) ->
    start(?MODULE, Options).
start(Name, Options) ->
    gen_server:start({local, Name}, ?MODULE, Options, []).

%%--------------------------------------------------------------------
%% @doc
%%    Starts server.
%% @end
%%-spec(start_link/3 :: (Transport :: atom(), Args :: list()) -> 
%%	     {ok, Pid :: pid()} | {error, Reason :: term()}).
start_link(Options) ->
    start_link(?MODULE, Options).

start_link(Name, Options) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []).

%% @doc
%%     Stops server.
%% @end
stop() ->
    stop(?MODULE).
-spec(stop/1 :: (Pid :: pid()) -> ok).
stop(Pid) ->
    (catch gen_server:call(Pid, stop)),
    ok.

%% @doc
%%    Returns the state of server.
%% @end
status() ->
    status(?MODULE).
-spec(status/1 :: (Pid :: atom() | pid()) -> 
	     {ok, Pid, #state{}} | {ok, not_running, Pid} 
		 | {error, Reason :: term()}). 
status(Pid)->
    try gen_server:call(Pid, status) of
	Result -> Result
	catch
 	  _:_Reason -> {ok, not_running, Pid}
	end.

%%==================================================
%% gen_server callbacks
%%==================================================
-spec(init/1 :: (Args :: [term()]) -> {ok, #state{}} 
					  | {ok, #state{}, Timeout :: integer()}
					  | {ok, #state{}, hibernate}
					  | {stop, reason()} 
					  | ignore).
init(Options) -> 
    Timeout = proplists:get_value(timeout, Options),
    case start_and_connect(Options) of
	{ok, Id}  -> 
%%	    ?debug("Started",[],init),
	    {ok, #state{pool_id = Id, odbc = whereis(Id), options = Options, 
			timeout = get_timeout(Timeout)}};
	Connect_Error -> {stop, Connect_Error}
    end.

%%--------------------------------------------------------------------
%% @doc
%%     Handling call messages. 
%%     Callback function that will be invoked by gen_server.
%% @end
-spec(handle_call/3 :: 
      (status, From :: pid(), #state{}) -> {reply, Reply :: term(), #state{}};
      (stop, From :: pid(), #state{}) -> {stop, normal, #state{}};
      (Request :: term(), From :: pid(), #state{}) -> 
	     {reply, {error, {unhandled_call, Request :: term(), From :: pid()}},
	      #state{}};
      (M :: term(), From :: pid(), #state{}) -> 
	     {reply, Reply :: term(), #state{}};
      (M :: term(), From :: pid(), #state{}) -> 
	     {reply, Reply :: term(), #state{}, Timeout :: integer()};
      (M :: term(), From :: pid(), #state{}) -> {noreply, #state{}};
      (M :: term(), From :: pid(), #state{}) -> 
	     {noreply, #state{}, Timeout :: integer()};
      (M :: term(), From :: pid(), #state{}) -> 
	     {stop, Reason :: term(), Reply :: term(), #state{}};
      (M :: term(), From :: pid(), #state{}) -> 
	     {stop, Reason :: term(), #state{}}).
%%--------------------------------------------------------------------

handle_call(status, _From, State) ->
    KVs = ?record_to_keyval(state, State),
    Reply={ok, self(), KVs},
    {reply, Reply, State};
handle_call(stop, _From, State) ->
    {stop, normal, State};
handle_call({create_table, Name, Opts}, _From, State) ->
    Reply = handle_create_table(Name, Opts, State),
    {reply, Reply, State};
%% handle_call({select, Statement, Options, FixedVals}, _From, State) ->
%%     Reply = handle_select(Statement, Options, FixedVals, State),
%%     {reply, Reply, State};
%% handle_call({update, Statement, Options}, _From, State) ->
%%     Reply = handle_update(Statement, Options, State),
%%     {reply, Reply, State};
handle_call({get_last_insert_id, Table, Options}, _From, State) ->
    Reply = handle_get_last_insert_id(Table, Options, State),
    {reply, Reply, State};
handle_call({describe_table, Table, Options}, _From, State) ->
    Reply = handle_describe_table(Table, Options, State),
    {reply, Reply, State};
handle_call({commit, _PoolId, Operation}, _From, #state{odbc = Ref} = State) ->
    Reply = odbc:commit(Ref, Operation),
    {reply, Reply, State};
handle_call({sql_query, Statement}, _From, 
	    #state{odbc = Ref, timeout = Timeout} = State) ->
    Reply = try odbc:sql_query(Ref, Statement, Timeout) of
		Result -> Result
	    catch _:Reason -> {error, Reason}
		       end,
    {reply, Reply, State};
%% handle_call({transaction, Fun, Options}, _From, State) ->
%%     Reply = handle_transaction(Fun, Options, State),
%%     {reply, Reply, State};
handle_call(Request, From, State) ->
    ?debug("Got unhandled call ~p from ~p.", [Request, From], handle_call),
    {reply, {error, {unhandled_call, Request, From}}, State}.

%%--------------------------------------------------------------------
%% @doc
%%     Handling cast messages. 
%%     Callback function that will be invoked by gen_server.
%% @end
-spec(handle_cast/2 :: 
      (status, #state{}) -> {reply, Reply :: term(), #state{}};
      (Request :: term(), #state{}) -> 
	     {reply, {error, {unhandled_call, Request :: term(), From :: pid()}},
	      #state{}};
      (M :: term(), #state{}) -> {noreply, #state{}};
      (M :: term(), #state{}) -> {noreply, #state{}, Timeout :: integer()};
      (M :: term(), #state{}) -> {noreply, #state{}, hibernate};
      (M :: term(), #state{}) -> {stop, Reason :: term(), #state{}}).
%%--------------------------------------------------------------------

handle_cast(Message, State) ->
    ?debug("Got unhandled info message ~p.", [Message], handle_cast),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc
%%     Handling all non call/cast messages. 
%%     Callback function that will be invoked by gen_server.
%% @end
%%--------------------------------------------------------------------
-spec(handle_info/2 :: 
      ({'EXIT', Pid :: pid(), normal}, State :: #state{}) -> {noreply, #state{}};
      (M :: term(), #state{}) -> {noreply, #state{}};
      (M :: term(), #state{}) -> {noreply, #state{}, Timeout :: integer()};
      (M :: term(), #state{}) -> {noreply, #state{}, hibernate};
      (M :: term(), #state{}) -> {stop, Reason :: term(), #state{}}).

handle_info(Message, State) ->
    ?debug("Got unhandled info message ~p.", [Message], handle_info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc
%%    This function is called by a gen_server when it is about to
%%    terminate. It should be the opposite of Module:init/1 and do any necessary
%%    cleaning up. When it returns, the gen_server terminates with Reason.
%%    The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec(terminate/2 :: (Reason :: term(), #state{}) -> ok).
terminate(_Reason, #state{odbc = Ref}) ->
    odbc:disconnect(Ref),
    ok.

%%--------------------------------------------------------------------
%% @doc 
%%    Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec(code_change/3 :: (OldVsn :: term(), #state{}, Extra :: term()) -> 
	     {ok, #state{}}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%==================================================
%% Internal functions
%%==================================================

start_and_connect(Options) ->
%%    ?debug("About to start ~p",[Options],start),
    application:load(odbc),
    application:start(odbc),
%%    Driver = proplists:get_value(driver, Options),
%%    Connect = connect_string(Driver, Options),
    Fun = proplists:get_value(connection_fun, Options),
    PoolId = proplists:get_value(pool_id, Options, get_default_pool_id()),
    case connect(PoolId, Fun, Options) of
  	{ok, _Ref} -> {ok, PoolId};
  	Reason -> Reason
    end.

%% @doc Create a connection to odbc with ConnectionId as an identifier
%%  Callback function connect_fun() must extract parameters from Options 
%% and return connection string.
%%  For ConnectString @see description of the function SQLDriverConnect in [1].
%%  [1] - Microsoft ODBC 3.0, Programmer's Reference and SDK Guide 
%% Example:
%% file_maker("DataDirect 32-BIT SequeLink 5.5" = Driver, Options) ->
%%    Port = proplists:get_value(port, Options),
%%    Hostname = proplists:get_value(hostname, Options),
%%    Username = proplists:get_value(username, Options),
%%    Password = proplists:get_value(password, Options),
%%    Database = proplists:get_value(db, Options),
%%    Connect = lists:flatten(io_lib:format(
%%			      "DRIVER={~p};HST=~p;PRT=~p;UID=~p;PWD=~p", 
%%			      [Driver, Hostname, Port, Username, Password])).
%% 1> Args = [{hostname, "127.0.0.1"},{username, "rfid"},{password, "rfid1234"},{port,1234}].
%% 2> Driver = [{driver, "DataDirect 32-BIT SequeLink 5.5"}].
%% next line is not working in shell
%% 3> Fun = [{connection_fun, fun fm:file_maker/2}]. 
%% 3> erlydb:start(odbc, Driver ++ Args ++ Fun).
-type(connection_fun() :: 
      fun((Driver :: string(), Options :: proplist()) -> string())).

-spec(connect/3 :: (ConnectionId :: atom(), 
		    Fun :: connection_fun(), 
		    Options :: proplist()) -> 
	     {ok, pool_ref()} | {error, reason()}).
connect(ConnectionId, Fun, Options) ->
    Driver_Dir = case proplists:get_value(driver_dir, Options) of
		     undefined -> [];
		     Dir -> [{driver_dir, Dir}]
		 end,
    case proplists:get_value(driver, Options) of
	undefined -> 
	    ?debug("Mandatory argument driver is missing.", [], connect),
	    {error, driver_not_specified};
	Driver -> 
%%	    ?debug("About to construct connect string ~p(~p, ~p).",
%%		   [Fun, Driver, Options],connect),
	    ConnectString = Fun(Driver, Options),
%%	    ?debug("Trying to connect: ~p", [ConnectString], connect),
	    case odbc:connect(ConnectString,
			      [{scrollable_cursor, off},
			       {auto_commit, off}] ++ Driver_Dir) of
		{ok, Ref} ->
		    catch erlang:unregister(ConnectionId),
		    erlang:register(ConnectionId, Ref),
		    erlang:monitor(process, ConnectionId),
%%		    ?debug("Success ~p ~p", [ConnectionId, Ref], connect),
		    {ok, ConnectionId};
		{error, _Reason} = Error -> 
		    ?debug("Cannot connect ~p, reason ~p", 
			   [ConnectionId, Error], connect),		    
		    Error
	    end
    end.

handle_create_table(Name, Opts, #state{odbc = Ref}) ->
    ?debug("About to create_table(~p, ~p)", [Name, Opts], handle_create_table),
    Encoder = case proplists:get_value(encode_fun, Opts) of
		  undefined -> fun decode/2;
		  Fun -> Fun
	      end,
    Types = proplists:get_value(user_properties, Opts),
%%    Def = string:join([format_field(T, Encoder) || T <- Types], ","),
    case format_fields(Types, Encoder) of
	{error, _Reason} = Error -> Error;
	Def ->
	    %% ODBC does not support brackets for quotation we have to use ""
	    SQL = lists:flatten(io_lib:format("CREATE TABLE \"~s\" (~s)",
					      [Name, Def])),
	    ?debug("SQL:~p", [SQL], create_table),
	    case odbc:sql_query(Ref, SQL) of
		{error, _Reason} = Error -> odbc:commit(Ref, rollback), Error;
		_Result -> odbc:commit(Ref, commit), {atomic, ok}
	    end				    
    end.

-type(esql() :: {esql, term()}).
-type(statement() :: esql() | binary() | string()).
%% @ doc Execute a SELECET statement.
%% -spec(handle_select/4 :: 
%%       (Statement :: statement(), Options :: proplist(), FixedVals :: list(),
%%        State :: #state{}) ->
%% 	     {ok, Rows :: integer()} | {error, reason()}).
%% handle_select(Statement, Options, FixedVals, #state{options = Parent}) ->
%%     get_select_result(q(Statement, Parent ++ Options), FixedVals).

%% @ doc Execute a DELETE or UPDATE statement.
%% -spec(handle_update/3 :: (Statement :: statement(), Options :: proplist(),
%% 			 State :: #state{}) -> 
%% 	     {ok, Affected :: integer()} | {error, reason()}).    
%% handle_update(Statement, Options, #state{options = Parent}) ->
%%     get_update_result(q(Statement, Parent ++ Options), []).

%% @doc Get the id of the last inserted record.
-spec(handle_get_last_insert_id/3 :: (Table :: atom(), Options :: proplist(),
				     State :: #state{}) -> 
	     {ok, Value :: term()}). %% FIXME now it is integer()
handle_get_last_insert_id(Table, Options, #state{options = Parent}) ->
    Opts = Parent ++ Options,
    ?debug("SQL: ~p",[q({esql, [{select, '*',{from, Table}}]}, Opts)],handle_get_last_insert_id),
    
    Res = odbc:select_count(get_pool_id(Opts), 
		      q({esql, [{select, '*',{from, Table}}]}, Opts)),
    ?debug("RES: ~p",[Res],handle_get_last_insert_id),
    Res.


%% @doc Get the table names and fields for the database.
%% Unfortunately current implementation of ODBC port driver doesn't support
%% ODBC standard SQLTables function so we cannot retrieve the list of the 
%% tables as a short term solution we will use Options to provide a list 
%% of tables. So we need to duplicate Modules in Options
%% Example:
%%  	erlydb:code_gen(['FM_tags', 'FM_projects'], odbc, 
%%            [{tables, ['FM_tags', 'FM_projects']}, {skip_fk_checks, true}]).
%% The same approach we using to specify primary_key due to missing 
%% SQLPrimaryKeys()
%% You can also use parameter calback_module
%% this module should export tables(odbc, PoolId)
-type(gb_trees() :: term()). %% @see gb_trees module
-spec(get_metadata/1 :: (Options :: proplist()) -> gb_trees()).
get_metadata(Options) ->
    PoolId = get_pool_id(Options),
    Tables = case proplists:get_value(callback_module, Options) of
		 undefined -> proplists:get_value(tables, Options);
		 Module -> Module:tables(?backend, PoolId)
	     end,
    lists:foldl(fun(Table, TablesTree) ->
			get_metadata(Table, PoolId, Options, TablesTree)
		end, gb_trees:empty(), Tables).

%%==================================================
%% Helper functions
%%==================================================
%%string:join([format_field(T, Decoder) || T <- Types], ","),
format_fields(Types, Decoder) ->
    format_fields(Types, Decoder, "").
format_fields([], _Decoder, Res) -> string:join(lists:reverse(Res), ",");
format_fields([Type | Rest], Decoder, Res) -> 
    case format_field(Type, Decoder) of
	{error, _Reason} = Error -> Error;
	String -> format_fields(Rest, Decoder, [String] ++ Res)
    end.    
    
format_field({Field, Type, Null, Key, Default, Extra, DBType}, Encoder) ->
    format_field({Field, Type, Null, Key, Default, Extra, DBType, []}, Encoder);

format_field({Field, Type, Null, _Key, _Default, _Extra, _DBType, _Attr}, 
	     Encoder) ->
    SQL_Null = case Null of
	       true -> "";
	       _ -> "NOT NULL"
	   end,
    case Encoder(field_type, Type) of
	{error, _Reason} = Error -> Error;
	SQL_Type -> io_lib:format("\"~s\" ~s ~s",[Field, SQL_Type, SQL_Null])
    end.

-spec(get_timeout/1 :: (Options :: 'undefined' | proplist()) -> integer()).
get_timeout(undefined) -> 50000;
get_timeout(Options) ->
    case proplists:get_value(erlydb_timeout, Options) of
	undefined -> 50000;
	Timeout -> Timeout
    end.

get_metadata(Table, _PoolId, Options, TablesTree) when is_atom(Table) ->
    Describer = case proplists:get_value(describe_table_fun, Options) of
		    undefined -> fun describe_table/2;
		    Fun when is_function(Fun) -> Fun
		end,
    case Describer(Table, Options) of
	{ok, Desc} -> 
	    Fields = new_fields(Desc, Options),
	    ?debug("DESC = ~p,FIELDS = ~p", 
		   [Desc, Fields], get_metadata),
	    gb_trees:enter(Table, Fields, TablesTree);
	Error -> exit(Error)
    end.


get_select_result({selected, _ColNames, SQLRes}, undefined) -> {ok, SQLRes};
get_select_result({selected, _ColNames, SQLRes}, []) -> {ok, SQLRes};
%% FIXME
get_select_result({selected, _ColNames, SQLRes}, FixedVals)->
    Result = lists:foldl(
	       fun(Fields, Acc) ->
		       Row = FixedVals ++
			   lists_to_binaries(tuple_to_list(Fields)),
		       [list_to_tuple(Row) | Acc]
	       end, [], SQLRes),
    {ok, Result};
get_select_result(Other, _) -> 
    Other.

lists_to_binaries(Row) ->
    [to_binary(Field) || Field <- Row].
    
to_binary(Field) when is_list(Field) -> 
    list_to_binary(Field);
to_binary(Row) -> Row.    

get_update_result({updated, NRows}, _FixedVals) -> 
    ?debug("NRows ~p",[NRows],get_update_result),
    {ok, NRows};
get_update_result({error, _Reason} = Error, _FixedVals) -> 
    ?debug("ERROR: ~p", [Error], get_update_result),
    Error;
get_update_result(List, _FixedVals) -> 
    ?debug("LIST: ~p", [List], get_update_result),    
    %% I hope there are no error tuples
    lists:foldl(fun({updated, X}, Sum) -> X + Sum;
		   ({selected, X, _Rows}, Sum) -> X + Sum
		end, 0, List).

-type(sql_type() :: 
      'SQL_CHAR'
      | 'SQL_VARCHAR'
      | 'SQL_NUMERIC'
      | 'SQL_DECIMAL'
      | 'SQL_INTEGER'
      | 'SQL_TINYINT'
      | 'SQL_SMALLINT'
      | 'SQL_REAL'
      | 'SQL_FLOAT'
      | 'SQL_DOUBLE'
      | 'SQL_BIT'
      | 'SQL_TYPE_DATE'
      | 'SQL_TYPE_TIME'
      | 'SQL_TYPE_TIMESTAMP'
      | 'SQL_BIGINT'
      | 'SQL_BINARY'
      | 'SQL_LONGVARCHAR'
      | 'SQL_VARBINARY'
      | 'SQL_LONGVARBINARY'
      | 'SQL_INTERVAL_MONTH'
      | 'SQL_INTERVAL_YEAR'
      | 'SQL_INTERVAL_DAY'
      | 'SQL_INTERVAL_MINUTE'
      | 'SQL_INTERVAL_HOUR_TO_SECOND'
      | 'SQL_INTERVAL_MINUTE_TO_SECOND'
      | 'SQL_UNKNOWN_TYPE'
     ).
-spec(describe_table/2 :: 
      (Table :: atom() | string(), Options :: proplist()) -> 
	     {ok, [{Field :: string(), Type :: sql_type()}]}).
%% describe_table(Table, Options) when is_atom(Table) -> %% NOT USED
%% handle_describe_table(Table, Options, State) when is_atom(Table) -> %% NOT USED
%%     case proplists:get_value(calback_module, Options) of
%% 		 undefined -> describe_table(atom_to_list(Table), Options);
%% 		 Module -> Module:describe_table(?backend, Table)
%%     end;
    
handle_describe_table(Table, Options, State) when is_atom(Table) -> 
    handle_describe_table(atom_to_list(Table), Options, State);

%% describe_table(Table, Options) when is_list(Table) -> 
handle_describe_table(Table, Options, _State) when is_list(Table) -> 
%%    io:format("About to describe table, ~p ~p", [Table, Options]),
    odbc:describe_table(get_pool_id(Options), Table, get_timeout(Options)).


%%-type(erlydb_field() :: term()). %% @see erlydb_field module
new_fields(Data, Options) ->
    Key = proplists:get_value(primary_key, Options, id),
    Decoder = case proplists:get_value(decode_fun, Options) of
		  undefined -> fun decode/2;
		  Fun -> Fun
	      end,
    new_fields(Data, atom_to_list(Key), Decoder, []).

new_fields([], _Key, _Decoder, Res) -> lists:reverse(Res);
new_fields([{Key, SQLType} | Rest], Key, Decoder, Res) -> 
    Type = Decoder(field_type, SQLType),
    Field = list_to_atom(Key),
    %%               new(Field, Type, NULL , Key    , Default  , Extra)
    N = erlydb_field:new(Field, Type, false, primary, undefined, undefined),
    new_fields(Rest, Key, Decoder, [N] ++ Res);
%% clause that we need when we are using describe_table_fun
new_fields([{Field, {Type, _}, Null, Key, Default, Extra, _DBType} | Rest], 
	   _Key, _Decoder, Res) -> 
    N = erlydb_field:new(Field, Type, Null, Key, Default, Extra),
    new_fields(Rest, _Key, _Decoder, [N] ++ Res);
new_fields([{Field, {Type, _}, Null, Key, Default, Extra, _DBType, Attributes} 
	    | Rest], 
	   _Key, _Decoder, Res) -> 
    N0 = erlydb_field:new(Field, Type, Null, Key, Default, Extra),
    ?debug("About to set attributes",[],new_fields),
    N1 = erlydb_field:attributes(N0, Attributes),
    new_fields(Rest, _Key, _Decoder, [N1] ++ Res);

new_fields([{Name, SQLType} | Rest], Key, Decoder, Res) -> 
    Type = Decoder(field_type, SQLType),
    Field = list_to_atom(Name),
    %%               new(Field, Type, NULL , Key      , Default  , Extra)
    N = erlydb_field:new(Field, Type, false, undefined, undefined, undefined),
    new_fields(Rest, Key, Decoder, [N] ++ Res).

%%decode_field_type(Type) -> decode(field_type, Type).
    
decode(field_type, 'SQL_CHAR') -> char;
decode(field_type, 'SQL_VARCHAR') -> varchar;
decode(field_type, {'sql_varchar', _Size}) -> varchar;
decode(field_type, 'SQL_NUMERIC') -> numeric;
decode(field_type, 'SQL_DECIMAL') -> decimal;
decode(field_type, 'SQL_INTEGER') -> integer;
decode(field_type, 'SQL_TINYINT') -> tinyint;
decode(field_type, 'SQL_SMALLINT') -> smallint;
decode(field_type, 'SQL_FLOAT') -> float;
decode(field_type, 'SQL_DOUBLE') -> double;
decode(field_type, 'SQL_BIT') -> bit;
decode(field_type, 'SQL_TYPE_DATE') -> date;
decode(field_type, 'SQL_TYPE_TIME') -> time;
decode(field_type, 'SQL_TYPE_TIMESTAMP') -> timestamp;
decode(field_type, 'SQL_BIGINT') -> bigint;
decode(field_type, 'SQL_BINARY') -> binary;
decode(field_type, 'SQL_VARBINARY') -> varbinary;
%% Types which are UNKNOWN for erlydb
decode(field_type, 'SQL_REAL') -> real; 
decode(field_type, 'SQL_LONGVARCHAR') -> longvarchar;
decode(field_type, 'SQL_LONGVARBINARY') -> longvarbinary;
decode(field_type, 'SQL_INTERVAL_MONTH') -> interval_month;
decode(field_type, 'SQL_INTERVAL_YEAR') -> interval_year;
decode(field_type, 'SQL_INTERVAL_DAY') -> interval_day;
decode(field_type, 'SQL_INTERVAL_MINUTE') -> interval_minute;
decode(field_type, 'SQL_INTERVAL_HOUR_TO_SECOND') -> interval_hour_to_second;
decode(field_type, 'SQL_INTERVAL_MINUTE_TO_SECOND') -> interval_minute_to_second;
decode(field_type, 'SQL_UNKNOWN_TYPE') -> unknown.
    
%% These function you should use when create_table
encode(field_type, {Type, undefined}) -> encode(field_type, Type);
encode(field_type, 'SQL_CHAR') -> "CHAR";
encode(field_type, 'SQL_VARCHAR') -> "VARCHAR";
encode(field_type, {'SQL_VARCHAR', Size}) -> 
    io_lib:format("VARCHAR (~p)",[Size]);
encode(field_type, 'SQL_NUMERIC') -> "NUMERIC";
encode(field_type, 'SQL_DECIMAL') -> "DECIMAL";
encode(field_type, 'SQL_INTEGER') -> "INTEGER";
encode(field_type, 'SQL_TINYINT') -> "TINYINT";
encode(field_type, 'SQL_SMALLINT') -> "SMALLINT";
encode(field_type, 'SQL_FLOAT') -> "FLOAT";
encode(field_type, 'SQL_DOUBLE') -> "DOUBLE";
encode(field_type, 'SQL_BIT') -> "BIT";
encode(field_type, 'SQL_TYPE_DATE') -> "TYPE_DATE";
encode(field_type, 'SQL_TYPE_TIME') -> "TYPE_TIME";
encode(field_type, 'SQL_TYPE_TIMESTAMP') -> "TYPE_TIMESTAMP";
encode(field_type, 'SQL_BIGINT') -> "BIGINT";
encode(field_type, 'SQL_BINARY') -> "BINARY";
encode(field_type, 'SQL_VARBINARY') -> "VARBINARY";
%% Types which are UNKNOWN for erlydb
encode(field_type, 'SQL_REAL') -> "REAL"; 
encode(field_type, 'SQL_LONGVARCHAR') -> "LONGVARCHAR";
encode(field_type, 'SQL_LONGVARBINARY') -> "LONGVARBINARY";
encode(field_type, 'SQL_INTERVAL_MONTH') -> "INTERVAL_MONTH";
encode(field_type, 'SQL_INTERVAL_YEAR') -> "INTERVAL_YEAR";
encode(field_type, 'SQL_INTERVAL_DAY') -> "INTERVAL_DAY";
encode(field_type, 'SQL_INTERVAL_MINUTE') -> "INTERVAL_MINUTE";
encode(field_type, 'SQL_INTERVAL_HOUR_TO_SECOND') -> "INTERVAL_HOUR_TO_SECOND";
encode(field_type, 'SQL_INTERVAL_MINUTE_TO_SECOND') -> 
    "INTERVAL_MINUTE_TO_SECOND";
encode(field_type, 'SQL_UNKNOWN_TYPE') -> "UNKNOWN".
    
%% @doc Execute a statement directly against the ODBC driver. If 
%%   Options contains the value {allow_unsafe_statements, true}, binary
%%   and string queries as well as ErlSQL queries with binary and/or
%%   string expressions are accepted. Otherwise, this function exits.
-type(odbc_result() :: term()). %% FIXME
-type(exception() :: term()). %% FIXME to exit({unsafe_statement, Statement})
%% FIXME maybe {error, reason()} also can be returned
-spec(q/2 :: (esql() | binary() | string(), Options :: proplist()) -> 
			 odbc_result() | exception()).
q({esql, Statement}, Options) ->
    ?debug("ESQL: ~p",[Statement], q),
    case allow_unsafe_statements(Options) of
	true -> 
	    ?debug("About to construct sql for ~p",[Statement], q),
	    q2(erlsql:unsafe_sql(Statement), Options);
	_ ->
	    case catch erlsql:sql(Statement) of
		{error, _} = Err -> exit(Err);
		Res -> q2(Res, Options)
	    end
    end;
q(Statement, Options) when is_binary(Statement); is_list(Statement) ->
    case allow_unsafe_statements(Options) of
	true -> q2(Statement, Options);
	_ -> exit({unsafe_statement, Statement})
    end.

%% @doc Execute a (binary or string) statement against the ODBC driver
%% using the default options.
%% ErlyDB doesn't use this function, but it's sometimes convenient for
%% testing.
-spec(q2/1 :: (binary() | string()) -> odbc_result() | exception()).
q2(Statement) ->
    q2(Statement, undefined).

%% @doc Execute a (binary or string) statement against the ODBC driver.
%% ErlyDB doesn't use this function, but it's sometimes convenient for testing.
%%
-spec(q2/2 :: (binary() | string(), Options :: proplist()) -> 
			 odbc_result() | exception()).
q2(Statement, Options) ->
    ?debug("SQL:~p",[lists:flatten(Statement)],q2),
    sql_query(Statement).

-spec(allow_unsafe_statements/1 :: (undefined | proplist()) -> bool()).
allow_unsafe_statements(undefined) -> true; %% erlsql cannot handle binaries
allow_unsafe_statements(Options) -> true.
%%    proplists:get_value(allow_unsafe_statements, Options).

-spec(get_default_pool_id/0 :: () -> atom()).
get_default_pool_id() -> list_to_atom(?MODULE_STRING ++ "_01").

-spec(get_pool_id/1 :: (undefined | proplist()) -> pid()).
get_pool_id(undefined) -> whereis(get_default_pool_id());
get_pool_id(Options) ->
    whereis(proplists:get_value(pool_id, Options, get_default_pool_id())).

%%==================================================
%% Test functions
%%==================================================
test(connect) -> 
    Args = [{hostname, "127.0.0.1"},{port,2399},
	    {username, "rfid"},{password, "rfid1234"}],
    Driver = [{driver, "DataDirect 32-BIT SequeLink 5.5"}],
    Fun = fun file_maker/2,
    erlydb:start(odbc, Driver ++ Args ++ [{connection_fun, Fun}] 
		 ++ [{driver_dir, "local/odbc-2.10.4/priv"}]),

    Tables = [rfid_tags], %% if it is an atom it will try to use code:which
    %% this is not working for erl_boot_loader
    %% so we will use local fs
    %%Tables = ["local/rfid_tags.erl"],
    erlydb:code_gen(Tables, odbc, [{tables, Tables}, {primary_key, tag_id},
				   {skip_fk_checks, true},
				   %% we need erlydb_base.erl
				   {src_dir, "local"}%%,
%%				  {allow_unsafe_statements, true}]), 
				   ]),
    Tag = term_to_binary([1,2,3,4,5,6,7,8,9,10,11,12]), 
    Reader = term_to_binary('127.0.0.1'),
    add_tag(Tag, Reader),
%%    io:format("rfid_tags:find() ~p~n",[rfid_tags:find()]),
    R = rfid_tags:find(
	  {
%%	   {tag_id, '=', erlsql:encode("1,2,3,4,5,6,7,8,9,10,11,12")},
	   {tag_id, '=', Tag},
	   'and',
%%	   {reader, '=', erlsql:encode("127.0.0.1")}
	   {reader, '=', Reader}
	  }),
    io:format("~p~n",[R]);

test(SQL) -> 
    Pool_ID = get_default_pool_id(),
    case sql_query(SQL) of
	{error, _Reason} = Error -> commit(Pool_ID, rollback), Error;
	_Result -> commit(Pool_ID, commit), {atomic, ok}
    end.

file_maker("DataDirect 32-BIT SequeLink 5.5" = Driver, Options) ->
    Port = proplists:get_value(port, Options),
    Hostname = proplists:get_value(hostname, Options),
    Username = proplists:get_value(username, Options),
    Password = proplists:get_value(password, Options),
    Connect = lists:flatten(
		io_lib:format("DRIVER={~s};HST=~s;PRT=~p;UID=~s;PWD=~s", 
			      [Driver, Hostname, Port, Username, Password])),
    Connect.

add_tag(Tag, Reader) ->
    Record = rfid_tags:new_with([{tag_id, Tag}, {reader, Reader}, {ts, now()}]),
    rfid_tags:transaction(
      fun() ->
	      rfid_tags:insert(Record) 
      end).
