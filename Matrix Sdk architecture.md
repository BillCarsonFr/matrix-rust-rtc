Matrix Sdk Architecture
crates:
matrix-rtc

- public classes
- `RTCSession`
  gets a list of sticky events and converts it into a session object.
  Its used in EX for computing room info.
  It has a static method and a stateful stream based emitter
  `RTC::Session::computeSessionsFromEvents(sticky:MatrixStickyEvents, state: MatrixStateEvents, memberState: MatrixStateEvents )-> RTC::Session[]`

```rust
mod RTC {
  computeSessionsFromEvents(sticky:MatrixStickyEvents, state: MatrixStateEvents, memberState: MatrixStateEvents )-> RTC::Session[]

  struct Session{
    transports: Transport[],
    members: RtcMember[],
  }
  impl Session{
    &mut updateSession(MatrixEvent: StickyMember|State|Slot)
    subscribe()
  }

  // consider adding a session manager that takes care of calling updateSession BUT probalby the participation Manager is the better suited place for this
  Member{
    member_id:
    display_name:
    avatar_url:
    intent:
  }

mod Connections {
    struct ConnectionWithMembers{
        pub conn: ConnectionData;
        pub members: Member[];
    }

    /** Returns the ConnectionData that should be used for publishing the local member
    * Sometimes it also does the jwt cs api request. Sometimes it returns an already existing token.
    * ConnectionDatas wsUrl can be treated as the index.
    */
    pub fn membersForConnectionData(connectionData, Session)-> Member[]

    struct ConnectionData {
        jwtToken: String;
        wsUlr: String;
    }

    struct Manager{
        session: &Session;
    }
    impl Manager {
        pub new(&Session);
        pub subscribe_connections() -> Stream<ConnectionData[]>;
        pub subscribe_connections_with_members()-> Stream<ConnectionWithMembers[]>;
        pub async add_own_transport(transport:Transport)-> ConnectionData;
    }
}

mod OwnMembership{
    struct JoinStatus{
        has_fetched_transport: boolean;
        has_fetched_initial_member_list: boolean;
        has_created_transport_token: boolean;
        has_sent_delayed_leave_event: boolen;
        has_sent_member_join_event: boolean;
        has_delegated_delayed_event: boolean;
        has_started_hearbeat: boolean;
    }
    struct ConnectedStatus{
        delayed_event_kick_ts: Option<number>;
        heartbeat_last_restart_ts: Option<number>;
        delegation_setup_ts: Option<number>;
    }
    struct LeaveStatus{
        lk_disconnected: boolean,
        leave_event_sent: boolean,
    }
    struct Manager{
    }
    impl Manager{
        // we pass the full driver but only as the trait. So that the manager does not get access to more than it needs.
        pub fn init(session: &Session, driver: MatrixDriver::OwnMembershipTrait, on_transport_created_callback: fn (transport: Transport)->());
        pub fn join();
        pub fn leave();
    }
}

mod Encryption{
    type KeyMap = HashMap<memberId: String, key: Key>

    struct JoinStatus{
        has_distributed_initial_keys: boolean,
        has_received_all_member_keys: boolean,
    }
    struct ConnectedStatus {
        // these need to be displayed as leaving so users know they might still listen
        left_members_with_keys: Member[];
        // actually fully secure call
        fully_settled: boolean;
        // this can be used to inform the user how much seconds of the call they will leak on join.
        // Should not be rendered directly. But can be used as a threshold to let users configure to not share
        // the current key with the price of a new joiner needing to wait. (until we have ratcheting)
        last_rotation_ts: number;
    }
    // The send machine chunks the call in time intervals. any session changes in those intervals result in key rotations
    struct SendMachine{
        next_rotation_check_ts: number;
    }
    impl SendMachine{
        pub fn new(
            driver: MatrixDriver::ToDeviceSendTrait,
            session: &Session,
            config: SendMachineConfig,
            on_key_for_own_member_change: fn(key)->()
        )
    }

    struct Machine{
        send_machine: SendMachine; // creates and manages it. Passes the driver parts, adds the on_key_for_own_member_change to update the `key_map`
        key_map: KeyMap; // stores all keys received over time
    }
    impl Machine{
        pub fn new(
            driver: MatrixDriver::ToDeviceTrait,
            session: &Session,
            own_member: Member,
            on_key_for_member_map_change: fn(key_map: &KeyMap) -> ());
    }
}

mod Participation{

    struct JoinStatus{
        ownMembership: OwnMembership::JoinStatus;
        encryption: Encryption::JoinStatus;
    }
    struct ConnectedStatus{
        ownMembership: OwnMembership::ConnectedStatus;
        encryption: Encryption::ConenctionStatus;
    }
    enum Status{
        Disconnected,
        Joining(JoinStatus),
        Connected(ConnectedStatus),
        Leaving(OwnMembership::LeaveStatus),
    }

    struct Manager{
        session: Session; // subscribe to driver and feed into session
        ownMembershipManager: OwnMembership::Manager; // gets the session setup and managed by the Particiption::Manager
        remoteConnectionManager: Connections::Manager; // gets the session and addOwnTransport will be called once ready
        encryptionManager: Encryption::Manager; // gets the session and the driver. Will send to-device messages and call on_key_for_member_map_change
    }
    impl Manager{
        // this is the MatrixDriver also used in the wiget codebase. No need to reinvent it.
        // its a perfect fit
        pub fn new(driver: MatrixDriver) -> Self;

        pub fn get_connections()-> &ConnectionWithMembers;
        pub fn on_connections_change(fn(connectins: &ConnectionWithMembers[]));

        pub fn get_key_map() -> &KeyMap;
        pub fn on_key_map_change(fn(key_map: &KeyMap) -> ());

        pub fn get_status() -> Status;
        pub fn on_status_change(fn(status: Status)->());
    }

}

```

## How to use

For in app status indication:

```rust
// inside the method that computes the room info for the timeline/room header (no timeline)
onRoomChange(){
  // member computation (session creation) is cheap enough so we can just put it inside each room change
  const sessions = computeSessionsFromEvents(currentEvents)
  updateRoomInfo([{sessions.memberCount, session.startTime , session.intent...},...])
}
```

For a call

```rust
let mut lk_rooms = {room: Room, members: Member[]}[];
onJoinPressed(session){
    let participation_manager = Participation::Manager::new(session, MatrixDriver::new(matrixRoom))
    participation_manager.join();
    // this should be doable with any lk sdk on any platform.
    participation_manager.on_connections_change((connection_data)->{
        const new_rooms = get_missing_lk_rooms(connection_data, &lk_rooms).map((missing_room_connection_data)=>{
            const r = Room::new(missing_room_connection_data.token,missing_room_connection_data.wsUrl);
            r.connect();
            {room: r, members: connection_data.members}
        })
        lk_rooms.push(new_rooms)
    });
    participation_manager.on_key_map_change((key_map, key_map_changes)->{
        for r in lk_rooms{
            for m in r.members{
                if key_map_changes.has(m.id){
                    r.room.set_key_for_participant(m.id, key_map[m.id])
                }
            }
        }
    })
}
```
