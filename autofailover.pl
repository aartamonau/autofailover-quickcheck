#!/usr/bin/swipl -q -s

:- initialization main.
:- dynamic mailed_too_small/1, mailed_down/1, node/2.

init_state() :-
    retractall(node(_, _)),
    retractall(mailed_too_small(_)),
    retractall(mailed_down(_)).

enough_nodes() :-
    all_nodes([_,_,_|_]).

is_up(Id) :-
    node(Id, down_state(_, up)).

is_down(Id) :-
    node(Id, _),
    \+is_up(Id).

in_state(State, Id) :-
    node(Id, down_state(_, State)).

all_nodes(Nodes) :-
    findall(Id, node(Id, _), Nodes).

down_nodes(DownNodes) :-
    findall(Id, is_down(Id), DownNodes).

failover_nodes(FailoverNodes) :-
    findall(Id, in_state(failover, Id), FailoverNodes).

action_failover(Node) :-
    enough_nodes(),
    failover_nodes([Node]),
    down_nodes([Node]).

action_mail_too_small(Node) :-
    all_nodes(AllNodes),
    \+mailed_too_small(AllNodes),

    \+enough_nodes(),
    failover_nodes([Node]),
    down_nodes([Node]),

    retractall(mailed_too_small(_)),
    asserta(mailed_too_small(AllNodes)).

action_mail_down_warning(Node) :-
    in_state(nearly_down, Node),
    is_down(OtherNode),
    Node \== OtherNode,

    \+mailed_down(Node),
    asserta(mailed_down(Node)).

actions(Actions) :-
    findall(action_failover(Id), action_failover(Id), Failovers),
    findall(action_mail_too_small(Id), action_mail_too_small(Id), TooSmall),
    findall(action_mail_down_warning(Id), action_mail_down_warning(Id), Down),
    append([Failovers, TooSmall, Down], Actions).

inc_down(down_state(0, up),           down_state(0, half_down)).
inc_down(down_state(0, half_down),    down_state(1, half_down)).
inc_down(down_state(1, half_down),    down_state(2, half_down)).
inc_down(down_state(2, half_down),    down_state(0, nearly_down)).
inc_down(down_state(0, nearly_down),  down_state(1, nearly_down)).
inc_down(down_state(1, nearly_down),  down_state(0, failover)).
inc_down(down_state(0, failover),     down_state(0, failover)).

new_node(Id) :-
    \+node(Id, _).

live_node(Id, AllNodes, DownNodes) :-
    member(Id, AllNodes),
    \+member(Id, DownNodes).

nodes_changed(NewNodes, OldNodes) :-
    subtract(NewNodes, OldNodes, Diff1),
    subtract(OldNodes, NewNodes, Diff2),
    ([_|_] = Diff1 ; [_|_] = Diff2).

new_state(Id, AllNodes, DownNodes, down_state(0, up)) :-
    live_node(Id, AllNodes, DownNodes).
new_state(Id, _, DownNodes, down_state(0, half_down)) :-
    member(Id, DownNodes),
    new_node(Id).
new_state(Id, _, DownNodes, down_state(0, nearly_down)) :-
    member(Id, DownNodes),
    [_,_|_] = DownNodes,
    node(Id, down_state(_, nearly_down)),
    !.
new_state(Id, _, DownNodes, NewState) :-
    member(Id, DownNodes),
    node(Id, OldState),
    inc_down(OldState, NewState).

process_node(Id, AllNodes, DownNodes) :-
    new_state(Id, AllNodes, DownNodes, NewState),
    retractall(node(Id, _)),
    asserta(node(Id, NewState)).
process_node(Id, AllNodes, DownNodes) :-
    \+new_state(Id, AllNodes, DownNodes, _),
    retractall(node(Id, _)).

reset_down_nodes() :-
    down_nodes(DownNodes),
    forall(member(Id, DownNodes),
           (retractall(node(Id, _)),
            asserta(node(Id, down_state(0, half_down))))).

process_frame(NewNodes, DownNodes) :-
    all_nodes(OldNodes),
    union(OldNodes, NewNodes, CombinedNodes),
    forall(member(Id, CombinedNodes), process_node(Id, NewNodes, DownNodes)),

    (nodes_changed(NewNodes, OldNodes) -> reset_down_nodes(); true).

main(_Args) :-
    prompt(_, ''),
    loop().

loop() :-
    read(Term),
    process_line(Term),
    loop().

process_line(end_of_file) :-
    halt().
process_line(frame(Nodes, Down)) :-
    process_frame(Nodes, Down),
    actions(Actions),
    include(action_of_type(failover), Actions, Failover),
    include(action_of_type(mail_too_small), Actions, MailTooSmall),
    include(action_of_type(mail_down_warning), Actions, MailDown),

    maplist(action_node, Failover, FailoverNodes),
    maplist(action_node, MailTooSmall, MailTooSmallNodes),
    maplist(action_node, MailDown, MailDownNodes),

    format("(~w,~w,~w)~n", [FailoverNodes, MailTooSmallNodes, MailDownNodes]).
process_line(Other) :-
    format("bad input term ~p~n", [Other]),
    halt(1).

action_node(action_failover(Id), Id).
action_node(action_mail_too_small(Id), Id).
action_node(action_mail_down_warning(Id), Id).

action_of_type(failover, action_failover(_)).
action_of_type(mail_too_small, action_mail_too_small(_)).
action_of_type(mail_down_warning, action_mail_down_warning(_)).
