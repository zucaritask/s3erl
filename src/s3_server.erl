%% @doc gen_server wrapping calls to s3. Contains configuration in
%% state, isolates caller from library failure, controls concurrency,
%% manages retries.
-module(s3_server).
-behaviour(gen_server).

-include("../include/s3.hrl").

%% API
-export([start_link/1,
         start_link/2,
         get_stats/0,
         get_stats/1,
         stop/0,
         stop/1,
         get_request_cost/0,
         get_request_cost/1]).

-export([default_max_concurrency_cb/1,
         default_retry_cb/2,
         default_post_request_cb/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(counters, {puts = 0, gets = 0, deletes = 0}).
-record(state, {config, workers, counters}).

%%
%% API
%%

start_link(Config) ->
    start_link({local, ?MODULE}, Config).
start_link(unnamed, Config) ->
    gen_server:start_link(?MODULE, Config, []);
start_link(ServerName, Config) ->
    gen_server:start_link(ServerName, ?MODULE, Config, []).

get_stats() ->
    get_stats(?MODULE).
get_stats(Pid) ->
    gen_server:call(Pid, get_stats).

get_request_cost() ->
    get_request_cost(?MODULE).
get_request_cost(Pid) ->
    {ok, Stats} = get_stats(Pid),
    GetCost = proplists:get_value(gets, Stats) / 1000000,
    PutCost = proplists:get_value(gets, Stats) / 100000,
    [{gets, GetCost}, {puts, PutCost}, {total, GetCost + PutCost}].

stop() ->
    stop(?MODULE).
stop(Pid) ->
    gen_server:call(Pid, stop).


%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    process_flag(trap_exit, true),

    AccessKey        = v(access_key, Config),
    SecretAccessKey  = v(secret_access_key, Config),
    Endpoint         = v(endpoint, Config),

    Timeout          = v(timeout, Config, 1500),
    RetryCallback    = v(retry_callback, Config,
                         fun ?MODULE:default_retry_cb/2),
    MaxRetries       = v(max_retries, Config, 3),
    RetryDelay       = v(retry_delay, Config, 500),
    MaxConcurrency   = v(max_concurrency, Config, 50),
    MaxConcurrencyCB = v(max_concurrency_callback, Config,
                         fun ?MODULE:default_max_concurrency_cb/1),
    PostRequestCB    = v(post_request_callback, Config,
                         fun ?MODULE:default_post_request_cb/3),
    ReturnHeaders    = v(return_headers, Config, false),

    C = #config{access_key         = AccessKey,
                secret_access_key  = SecretAccessKey,
                endpoint           = Endpoint,
                timeout            = Timeout,
                retry_callback     = RetryCallback,
                max_retries        = MaxRetries,
                retry_delay        = RetryDelay,
                max_concurrency    = MaxConcurrency,
                max_concurrency_cb = MaxConcurrencyCB,
                post_request_cb    = PostRequestCB,
                return_headers     = ReturnHeaders},

    {ok, #state{config = C, workers = [], counters = #counters{}}}.

handle_call({request, Req}, From, #state{config = C} = State)
  when length(State#state.workers) < C#config.max_concurrency ->
    WorkerPid =
        spawn_link(fun() ->
                           gen_server:reply(From, handle_request(Req, C))
                   end),
    NewState = State#state{workers = [WorkerPid | State#state.workers],
                           counters = update_counters(Req, State#state.counters)},
    {noreply, NewState};

handle_call({request, _}, _From, #state{config = C} = State)
  when length(State#state.workers) >= C#config.max_concurrency ->
    (C#config.max_concurrency_cb)(C#config.max_concurrency),
    {reply, {error, max_concurrency}, State};

handle_call(get_num_workers, _From, #state{workers = Workers} = State) ->
    {reply, length(Workers), State};

handle_call(get_stats, _From, #state{workers = Workers, counters = C} = State) ->
    Stats = [{puts, C#counters.puts},
             {gets, C#counters.gets},
             {deletes, C#counters.deletes},
             {num_workers, length(Workers)}],
    {reply, {ok, Stats}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _}, State) ->
    %% TODO: Keep track of From references in workers list so we can
    %% reply to our caller
    case lists:member(Pid, State#state.workers) of
        true ->
            NewWorkers = lists:delete(Pid, State#state.workers),
            {noreply, State#state{workers = NewWorkers}};
        false ->
            error_logger:info_msg("ignored down message~n"),
            {noreply, State}
    end;

handle_info(Info, State) ->
    error_logger:info_msg("ignored: ~p~n", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

v(Key, Data) ->
    proplists:get_value(Key, Data).

v(Key, Data, Default) ->
    proplists:get_value(Key, Data, Default).

%% @doc: Executes the given request, will retry if request failed
handle_request(Req, C) ->
    handle_request(Req, C, 0).

handle_request(Req, C, Attempts) ->
    Start = os:timestamp(),
    case catch execute_request(Req, C) of
        %% Continue trying if we have connection related errors
        {error, Reason} when Attempts < C#config.max_retries andalso
                             (Reason =:= connect_timeout orelse
                              Reason =:= timeout) ->
            catch (C#config.retry_callback)(Reason, Attempts),
            timer:sleep(C#config.retry_delay),
            handle_request(Req, C, Attempts + 1);

        {'EXIT', {econnrefused, _}} when Attempts < C#config.max_retries ->
            error_logger:info_msg("exit: ~p~n", [{Req, Attempts}]),
            catch (C#config.retry_callback)(econnrefused, Attempts),
            timer:sleep(C#config.retry_delay),
            handle_request(Req, C, Attempts + 1);

        {error, {"InternalError", _}} ->
            catch (C#config.retry_callback)(internal_error, Attempts),
            timer:sleep(C#config.retry_delay),
            handle_request(Req, C, Attempts + 1);

        {error, {503, "Service Unavailable"}} ->
            catch (C#config.retry_callback)(internal_error, Attempts),
            timer:sleep(C#config.retry_delay),
            handle_request(Req, C, Attempts + 1);

        Res ->
            End = os:timestamp(),
            catch (C#config.post_request_cb)(Req, Res, timer:now_diff(End, Start)),
            Res
    end.

execute_request({get, Bucket, Key, Headers}, C) ->
    s3_lib:get(C, Bucket, Key, Headers);
execute_request({put, Bucket, Key, Value, ContentType, Headers}, C) ->
    s3_lib:put(C, Bucket, Key, Value, ContentType, Headers);
execute_request({delete, Bucket, Key}, C) ->
    s3_lib:delete(C, Bucket, Key);
execute_request({head, Bucket, Key, Headers}, C) ->
    s3_lib:head(C, Bucket, Key, Headers);
execute_request({list, Bucket, Prefix, MaxKeys, Marker}, C) ->
    s3_lib:list(C, Bucket, Prefix, MaxKeys, Marker);
execute_request({list_details, Bucket, Prefix, MaxKeys, Marker}, C) ->
    s3_lib:list_details(C, Bucket, Prefix, MaxKeys, Marker).


request_method({get, _, _, _})             -> get;
request_method({put, _, _, _, _, _})       -> put;
request_method({delete, _, _})             -> delete;
request_method({head, _, _, _})            -> head;
request_method({list, _, _, _, _})         -> get;
request_method({list_details, _, _, _, _}) -> get.


update_counters(Req, Cs) ->
    case request_method(Req) of
        get    -> Cs#counters{gets = Cs#counters.gets + 1};
        put    -> Cs#counters{puts = Cs#counters.puts + 1};
        delete -> Cs#counters{deletes = Cs#counters.deletes + 1};
        head   -> Cs#counters{gets = Cs#counters.gets + 1}
    end.

default_max_concurrency_cb(_)    -> ok.
default_retry_cb(_, _)           -> ok.
default_post_request_cb(_, _, _) -> ok.
