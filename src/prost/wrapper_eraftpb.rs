impl Entry {
    pub fn new_() -> Entry {
        ::std::default::Default::default()
    }
    pub fn clear_entry_type(&mut self) {
        self.entry_type = 0
    }
    pub fn set_entry_type_(&mut self, v: EntryType) {
        self.entry_type = unsafe { ::std::mem::transmute::<EntryType, i32>(v) };
    }
    pub fn get_entry_type(&self) -> EntryType {
        unsafe { ::std::mem::transmute::<i32, EntryType>(self.entry_type) }
    }
    pub fn clear_term(&mut self) {
        self.term = 0
    }
    pub fn set_term(&mut self, v: u64) {
        self.term = v;
    }
    pub fn get_term(&self) -> u64 {
        self.term
    }
    pub fn clear_index(&mut self) {
        self.index = 0
    }
    pub fn set_index(&mut self, v: u64) {
        self.index = v;
    }
    pub fn get_index(&self) -> u64 {
        self.index
    }
    pub fn clear_data(&mut self) {
        self.data.clear();
    }
    pub fn set_data(&mut self, v: std::vec::Vec<u8>) {
        self.data = v;
    }
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }
    pub fn mut_data(&mut self) -> &mut std::vec::Vec<u8> {
        &mut self.data
    }
    pub fn take_data(&mut self) -> std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.data, ::std::vec::Vec::new())
    }
    pub fn clear_context(&mut self) {
        self.context.clear();
    }
    pub fn set_context(&mut self, v: std::vec::Vec<u8>) {
        self.context = v;
    }
    pub fn get_context(&self) -> &[u8] {
        &self.context
    }
    pub fn mut_context(&mut self) -> &mut std::vec::Vec<u8> {
        &mut self.context
    }
    pub fn take_context(&mut self) -> std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.context, ::std::vec::Vec::new())
    }
    pub fn clear_sync_log(&mut self) {
        self.sync_log = false
    }
    pub fn set_sync_log(&mut self, v: bool) {
        self.sync_log = v;
    }
    pub fn get_sync_log(&self) -> bool {
        self.sync_log
    }
}
impl ::protobuf::Clear for Entry {
    fn clear(&mut self) {
        ::prost::Message::clear(self);
    }
}
impl ::protobuf::Message for Entry {
    fn compute_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn get_cached_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }
    fn new() -> Self {
        Self::new_()
    }
    fn write_to_with_cached_sizes(
        &self,
        _os: &mut ::protobuf::CodedOutputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn default_instance() -> &'static Entry {
        ::lazy_static::lazy_static! {
            static ref INSTANCE: Entry = Entry::new_();
        }
        &*INSTANCE
    }
    fn is_initialized(&self) -> bool {
        true
    }
    fn merge_from(
        &mut self,
        _is: &mut ::protobuf::CodedInputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        unimplemented!();
    }
    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        unimplemented!();
    }
    fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        if let Err(_) = ::prost::Message::encode(self, &mut buf) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(buf)
    }
    fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
        if let Err(_) = ::prost::Message::merge(self, bytes) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(())
    }
}
impl SnapshotMetadata {
    pub fn new_() -> SnapshotMetadata {
        ::std::default::Default::default()
    }
    pub fn has_conf_state(&self) -> bool {
        self.conf_state.is_some()
    }
    pub fn clear_conf_state(&mut self) {
        self.conf_state = ::std::option::Option::None
    }
    pub fn set_conf_state(&mut self, v: ConfState) {
        self.conf_state = ::std::option::Option::Some(v);
    }
    pub fn get_conf_state(&self) -> &ConfState {
        match self.conf_state.as_ref() {
            Some(v) => v,
            None => <ConfState as ::protobuf::Message>::default_instance(),
        }
    }
    pub fn mut_conf_state(&mut self) -> &mut ConfState {
        if self.conf_state.is_none() {
            self.conf_state = ::std::option::Option::Some(ConfState::default());
        }
        self.conf_state.as_mut().unwrap()
    }
    pub fn take_conf_state(&mut self) -> ConfState {
        self.conf_state.take().unwrap_or_else(ConfState::default)
    }
    pub fn has_pending_membership_change(&self) -> bool {
        self.pending_membership_change.is_some()
    }
    pub fn clear_pending_membership_change(&mut self) {
        self.pending_membership_change = ::std::option::Option::None
    }
    pub fn set_pending_membership_change(&mut self, v: ConfState) {
        self.pending_membership_change = ::std::option::Option::Some(v);
    }
    pub fn get_pending_membership_change(&self) -> &ConfState {
        match self.pending_membership_change.as_ref() {
            Some(v) => v,
            None => <ConfState as ::protobuf::Message>::default_instance(),
        }
    }
    pub fn mut_pending_membership_change(&mut self) -> &mut ConfState {
        if self.pending_membership_change.is_none() {
            self.pending_membership_change = ::std::option::Option::Some(ConfState::default());
        }
        self.pending_membership_change.as_mut().unwrap()
    }
    pub fn take_pending_membership_change(&mut self) -> ConfState {
        self.pending_membership_change
            .take()
            .unwrap_or_else(ConfState::default)
    }
    pub fn clear_pending_membership_change_index(&mut self) {
        self.pending_membership_change_index = 0
    }
    pub fn set_pending_membership_change_index(&mut self, v: u64) {
        self.pending_membership_change_index = v;
    }
    pub fn get_pending_membership_change_index(&self) -> u64 {
        self.pending_membership_change_index
    }
    pub fn clear_index(&mut self) {
        self.index = 0
    }
    pub fn set_index(&mut self, v: u64) {
        self.index = v;
    }
    pub fn get_index(&self) -> u64 {
        self.index
    }
    pub fn clear_term(&mut self) {
        self.term = 0
    }
    pub fn set_term(&mut self, v: u64) {
        self.term = v;
    }
    pub fn get_term(&self) -> u64 {
        self.term
    }
}
impl ::protobuf::Clear for SnapshotMetadata {
    fn clear(&mut self) {
        ::prost::Message::clear(self);
    }
}
impl ::protobuf::Message for SnapshotMetadata {
    fn compute_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn get_cached_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }
    fn new() -> Self {
        Self::new_()
    }
    fn write_to_with_cached_sizes(
        &self,
        _os: &mut ::protobuf::CodedOutputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn default_instance() -> &'static SnapshotMetadata {
        ::lazy_static::lazy_static! {
            static ref INSTANCE: SnapshotMetadata = SnapshotMetadata::new_();
        }
        &*INSTANCE
    }
    fn is_initialized(&self) -> bool {
        true
    }
    fn merge_from(
        &mut self,
        _is: &mut ::protobuf::CodedInputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        unimplemented!();
    }
    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        unimplemented!();
    }
    fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        if let Err(_) = ::prost::Message::encode(self, &mut buf) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(buf)
    }
    fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
        if let Err(_) = ::prost::Message::merge(self, bytes) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(())
    }
}
impl Snapshot {
    pub fn new_() -> Snapshot {
        ::std::default::Default::default()
    }
    pub fn clear_data(&mut self) {
        self.data.clear();
    }
    pub fn set_data(&mut self, v: std::vec::Vec<u8>) {
        self.data = v;
    }
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }
    pub fn mut_data(&mut self) -> &mut std::vec::Vec<u8> {
        &mut self.data
    }
    pub fn take_data(&mut self) -> std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.data, ::std::vec::Vec::new())
    }
    pub fn has_metadata(&self) -> bool {
        self.metadata.is_some()
    }
    pub fn clear_metadata(&mut self) {
        self.metadata = ::std::option::Option::None
    }
    pub fn set_metadata(&mut self, v: SnapshotMetadata) {
        self.metadata = ::std::option::Option::Some(v);
    }
    pub fn get_metadata(&self) -> &SnapshotMetadata {
        match self.metadata.as_ref() {
            Some(v) => v,
            None => <SnapshotMetadata as ::protobuf::Message>::default_instance(),
        }
    }
    pub fn mut_metadata(&mut self) -> &mut SnapshotMetadata {
        if self.metadata.is_none() {
            self.metadata = ::std::option::Option::Some(SnapshotMetadata::default());
        }
        self.metadata.as_mut().unwrap()
    }
    pub fn take_metadata(&mut self) -> SnapshotMetadata {
        self.metadata
            .take()
            .unwrap_or_else(SnapshotMetadata::default)
    }
}
impl ::protobuf::Clear for Snapshot {
    fn clear(&mut self) {
        ::prost::Message::clear(self);
    }
}
impl ::protobuf::Message for Snapshot {
    fn compute_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn get_cached_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }
    fn new() -> Self {
        Self::new_()
    }
    fn write_to_with_cached_sizes(
        &self,
        _os: &mut ::protobuf::CodedOutputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn default_instance() -> &'static Snapshot {
        ::lazy_static::lazy_static! {
            static ref INSTANCE: Snapshot = Snapshot::new_();
        }
        &*INSTANCE
    }
    fn is_initialized(&self) -> bool {
        true
    }
    fn merge_from(
        &mut self,
        _is: &mut ::protobuf::CodedInputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        unimplemented!();
    }
    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        unimplemented!();
    }
    fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        if let Err(_) = ::prost::Message::encode(self, &mut buf) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(buf)
    }
    fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
        if let Err(_) = ::prost::Message::merge(self, bytes) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(())
    }
}
impl Message {
    pub fn new_() -> Message {
        ::std::default::Default::default()
    }
    pub fn clear_msg_type(&mut self) {
        self.msg_type = 0
    }
    pub fn set_msg_type_(&mut self, v: MessageType) {
        self.msg_type = unsafe { ::std::mem::transmute::<MessageType, i32>(v) };
    }
    pub fn get_msg_type(&self) -> MessageType {
        unsafe { ::std::mem::transmute::<i32, MessageType>(self.msg_type) }
    }
    pub fn clear_to(&mut self) {
        self.to = 0
    }
    pub fn set_to(&mut self, v: u64) {
        self.to = v;
    }
    pub fn get_to(&self) -> u64 {
        self.to
    }
    pub fn clear_from(&mut self) {
        self.from = 0
    }
    pub fn set_from(&mut self, v: u64) {
        self.from = v;
    }
    pub fn get_from(&self) -> u64 {
        self.from
    }
    pub fn clear_term(&mut self) {
        self.term = 0
    }
    pub fn set_term(&mut self, v: u64) {
        self.term = v;
    }
    pub fn get_term(&self) -> u64 {
        self.term
    }
    pub fn clear_log_term(&mut self) {
        self.log_term = 0
    }
    pub fn set_log_term(&mut self, v: u64) {
        self.log_term = v;
    }
    pub fn get_log_term(&self) -> u64 {
        self.log_term
    }
    pub fn clear_index(&mut self) {
        self.index = 0
    }
    pub fn set_index(&mut self, v: u64) {
        self.index = v;
    }
    pub fn get_index(&self) -> u64 {
        self.index
    }
    pub fn clear_entries(&mut self) {
        self.entries.clear();
    }
    pub fn set_entries(&mut self, v: ::std::vec::Vec<Entry>) {
        self.entries = v;
    }
    pub fn get_entries(&self) -> &::std::vec::Vec<Entry> {
        &self.entries
    }
    pub fn mut_entries(&mut self) -> &mut ::std::vec::Vec<Entry> {
        &mut self.entries
    }
    pub fn take_entries(&mut self) -> ::std::vec::Vec<Entry> {
        ::std::mem::replace(&mut self.entries, ::std::vec::Vec::new())
    }
    pub fn clear_commit(&mut self) {
        self.commit = 0
    }
    pub fn set_commit(&mut self, v: u64) {
        self.commit = v;
    }
    pub fn get_commit(&self) -> u64 {
        self.commit
    }
    pub fn has_snapshot(&self) -> bool {
        self.snapshot.is_some()
    }
    pub fn clear_snapshot(&mut self) {
        self.snapshot = ::std::option::Option::None
    }
    pub fn set_snapshot(&mut self, v: Snapshot) {
        self.snapshot = ::std::option::Option::Some(v);
    }
    pub fn get_snapshot(&self) -> &Snapshot {
        match self.snapshot.as_ref() {
            Some(v) => v,
            None => <Snapshot as ::protobuf::Message>::default_instance(),
        }
    }
    pub fn mut_snapshot(&mut self) -> &mut Snapshot {
        if self.snapshot.is_none() {
            self.snapshot = ::std::option::Option::Some(Snapshot::default());
        }
        self.snapshot.as_mut().unwrap()
    }
    pub fn take_snapshot(&mut self) -> Snapshot {
        self.snapshot.take().unwrap_or_else(Snapshot::default)
    }
    pub fn clear_reject(&mut self) {
        self.reject = false
    }
    pub fn set_reject(&mut self, v: bool) {
        self.reject = v;
    }
    pub fn get_reject(&self) -> bool {
        self.reject
    }
    pub fn clear_reject_hint(&mut self) {
        self.reject_hint = 0
    }
    pub fn set_reject_hint(&mut self, v: u64) {
        self.reject_hint = v;
    }
    pub fn get_reject_hint(&self) -> u64 {
        self.reject_hint
    }
    pub fn clear_context(&mut self) {
        self.context.clear();
    }
    pub fn set_context(&mut self, v: std::vec::Vec<u8>) {
        self.context = v;
    }
    pub fn get_context(&self) -> &[u8] {
        &self.context
    }
    pub fn mut_context(&mut self) -> &mut std::vec::Vec<u8> {
        &mut self.context
    }
    pub fn take_context(&mut self) -> std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.context, ::std::vec::Vec::new())
    }
}
impl ::protobuf::Clear for Message {
    fn clear(&mut self) {
        ::prost::Message::clear(self);
    }
}
impl ::protobuf::Message for Message {
    fn compute_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn get_cached_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }
    fn new() -> Self {
        Self::new_()
    }
    fn write_to_with_cached_sizes(
        &self,
        _os: &mut ::protobuf::CodedOutputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn default_instance() -> &'static Message {
        ::lazy_static::lazy_static! {
            static ref INSTANCE: Message = Message::new_();
        }
        &*INSTANCE
    }
    fn is_initialized(&self) -> bool {
        true
    }
    fn merge_from(
        &mut self,
        _is: &mut ::protobuf::CodedInputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        unimplemented!();
    }
    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        unimplemented!();
    }
    fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        if let Err(_) = ::prost::Message::encode(self, &mut buf) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(buf)
    }
    fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
        if let Err(_) = ::prost::Message::merge(self, bytes) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(())
    }
}
impl HardState {
    pub fn new_() -> HardState {
        ::std::default::Default::default()
    }
    pub fn clear_term(&mut self) {
        self.term = 0
    }
    pub fn set_term(&mut self, v: u64) {
        self.term = v;
    }
    pub fn get_term(&self) -> u64 {
        self.term
    }
    pub fn clear_vote(&mut self) {
        self.vote = 0
    }
    pub fn set_vote(&mut self, v: u64) {
        self.vote = v;
    }
    pub fn get_vote(&self) -> u64 {
        self.vote
    }
    pub fn clear_commit(&mut self) {
        self.commit = 0
    }
    pub fn set_commit(&mut self, v: u64) {
        self.commit = v;
    }
    pub fn get_commit(&self) -> u64 {
        self.commit
    }
}
impl ::protobuf::Clear for HardState {
    fn clear(&mut self) {
        ::prost::Message::clear(self);
    }
}
impl ::protobuf::Message for HardState {
    fn compute_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn get_cached_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }
    fn new() -> Self {
        Self::new_()
    }
    fn write_to_with_cached_sizes(
        &self,
        _os: &mut ::protobuf::CodedOutputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn default_instance() -> &'static HardState {
        ::lazy_static::lazy_static! {
            static ref INSTANCE: HardState = HardState::new_();
        }
        &*INSTANCE
    }
    fn is_initialized(&self) -> bool {
        true
    }
    fn merge_from(
        &mut self,
        _is: &mut ::protobuf::CodedInputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        unimplemented!();
    }
    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        unimplemented!();
    }
    fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        if let Err(_) = ::prost::Message::encode(self, &mut buf) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(buf)
    }
    fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
        if let Err(_) = ::prost::Message::merge(self, bytes) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(())
    }
}
impl ConfState {
    pub fn new_() -> ConfState {
        ::std::default::Default::default()
    }
    pub fn clear_nodes(&mut self) {
        self.nodes.clear();
    }
    pub fn set_nodes(&mut self, v: ::std::vec::Vec<u64>) {
        self.nodes = v;
    }
    pub fn get_nodes(&self) -> &::std::vec::Vec<u64> {
        &self.nodes
    }
    pub fn mut_nodes(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.nodes
    }
    pub fn take_nodes(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.nodes, ::std::vec::Vec::new())
    }
    pub fn clear_learners(&mut self) {
        self.learners.clear();
    }
    pub fn set_learners(&mut self, v: ::std::vec::Vec<u64>) {
        self.learners = v;
    }
    pub fn get_learners(&self) -> &::std::vec::Vec<u64> {
        &self.learners
    }
    pub fn mut_learners(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.learners
    }
    pub fn take_learners(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.learners, ::std::vec::Vec::new())
    }
}
impl ::protobuf::Clear for ConfState {
    fn clear(&mut self) {
        ::prost::Message::clear(self);
    }
}
impl ::protobuf::Message for ConfState {
    fn compute_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn get_cached_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }
    fn new() -> Self {
        Self::new_()
    }
    fn write_to_with_cached_sizes(
        &self,
        _os: &mut ::protobuf::CodedOutputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn default_instance() -> &'static ConfState {
        ::lazy_static::lazy_static! {
            static ref INSTANCE: ConfState = ConfState::new_();
        }
        &*INSTANCE
    }
    fn is_initialized(&self) -> bool {
        true
    }
    fn merge_from(
        &mut self,
        _is: &mut ::protobuf::CodedInputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        unimplemented!();
    }
    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        unimplemented!();
    }
    fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        if let Err(_) = ::prost::Message::encode(self, &mut buf) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(buf)
    }
    fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
        if let Err(_) = ::prost::Message::merge(self, bytes) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(())
    }
}
impl ConfChange {
    pub fn new_() -> ConfChange {
        ::std::default::Default::default()
    }
    pub fn clear_id(&mut self) {
        self.id = 0
    }
    pub fn set_id(&mut self, v: u64) {
        self.id = v;
    }
    pub fn get_id(&self) -> u64 {
        self.id
    }
    pub fn clear_change_type(&mut self) {
        self.change_type = 0
    }
    pub fn set_change_type_(&mut self, v: ConfChangeType) {
        self.change_type = unsafe { ::std::mem::transmute::<ConfChangeType, i32>(v) };
    }
    pub fn get_change_type(&self) -> ConfChangeType {
        unsafe { ::std::mem::transmute::<i32, ConfChangeType>(self.change_type) }
    }
    pub fn clear_node_id(&mut self) {
        self.node_id = 0
    }
    pub fn set_node_id(&mut self, v: u64) {
        self.node_id = v;
    }
    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }
    pub fn clear_context(&mut self) {
        self.context.clear();
    }
    pub fn set_context(&mut self, v: std::vec::Vec<u8>) {
        self.context = v;
    }
    pub fn get_context(&self) -> &[u8] {
        &self.context
    }
    pub fn mut_context(&mut self) -> &mut std::vec::Vec<u8> {
        &mut self.context
    }
    pub fn take_context(&mut self) -> std::vec::Vec<u8> {
        ::std::mem::replace(&mut self.context, ::std::vec::Vec::new())
    }
    pub fn has_configuration(&self) -> bool {
        self.configuration.is_some()
    }
    pub fn clear_configuration(&mut self) {
        self.configuration = ::std::option::Option::None
    }
    pub fn set_configuration(&mut self, v: ConfState) {
        self.configuration = ::std::option::Option::Some(v);
    }
    pub fn get_configuration(&self) -> &ConfState {
        match self.configuration.as_ref() {
            Some(v) => v,
            None => <ConfState as ::protobuf::Message>::default_instance(),
        }
    }
    pub fn mut_configuration(&mut self) -> &mut ConfState {
        if self.configuration.is_none() {
            self.configuration = ::std::option::Option::Some(ConfState::default());
        }
        self.configuration.as_mut().unwrap()
    }
    pub fn take_configuration(&mut self) -> ConfState {
        self.configuration.take().unwrap_or_else(ConfState::default)
    }
    pub fn clear_start_index(&mut self) {
        self.start_index = 0
    }
    pub fn set_start_index(&mut self, v: u64) {
        self.start_index = v;
    }
    pub fn get_start_index(&self) -> u64 {
        self.start_index
    }
}
impl ::protobuf::Clear for ConfChange {
    fn clear(&mut self) {
        ::prost::Message::clear(self);
    }
}
impl ::protobuf::Message for ConfChange {
    fn compute_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn get_cached_size(&self) -> u32 {
        ::prost::Message::encoded_len(self) as u32
    }
    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        Self::descriptor_static()
    }
    fn new() -> Self {
        Self::new_()
    }
    fn write_to_with_cached_sizes(
        &self,
        _os: &mut ::protobuf::CodedOutputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn default_instance() -> &'static ConfChange {
        ::lazy_static::lazy_static! {
            static ref INSTANCE: ConfChange = ConfChange::new_();
        }
        &*INSTANCE
    }
    fn is_initialized(&self) -> bool {
        true
    }
    fn merge_from(
        &mut self,
        _is: &mut ::protobuf::CodedInputStream,
    ) -> ::protobuf::ProtobufResult<()> {
        unimplemented!();
    }
    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        unimplemented!();
    }
    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        unimplemented!();
    }
    fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
        let mut buf = Vec::new();
        if let Err(_) = ::prost::Message::encode(self, &mut buf) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(buf)
    }
    fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
        if let Err(_) = ::prost::Message::merge(self, bytes) {
            return Err(::protobuf::ProtobufError::WireError(
                ::protobuf::error::WireError::Other,
            ));
        }
        Ok(())
    }
}
impl EntryType {
    pub fn values() -> &'static [Self] {
        static VALUES: &'static [EntryType] = &[EntryType::EntryNormal, EntryType::EntryConfChange];
        VALUES
    }
}
impl MessageType {
    pub fn values() -> &'static [Self] {
        static VALUES: &'static [MessageType] = &[
            MessageType::MsgHup,
            MessageType::MsgBeat,
            MessageType::MsgPropose,
            MessageType::MsgAppend,
            MessageType::MsgAppendResponse,
            MessageType::MsgRequestVote,
            MessageType::MsgRequestVoteResponse,
            MessageType::MsgSnapshot,
            MessageType::MsgHeartbeat,
            MessageType::MsgHeartbeatResponse,
            MessageType::MsgUnreachable,
            MessageType::MsgSnapStatus,
            MessageType::MsgCheckQuorum,
            MessageType::MsgTransferLeader,
            MessageType::MsgTimeoutNow,
            MessageType::MsgReadIndex,
            MessageType::MsgReadIndexResp,
            MessageType::MsgRequestPreVote,
            MessageType::MsgRequestPreVoteResponse,
        ];
        VALUES
    }
}
impl ConfChangeType {
    pub fn values() -> &'static [Self] {
        static VALUES: &'static [ConfChangeType] = &[
            ConfChangeType::AddNode,
            ConfChangeType::RemoveNode,
            ConfChangeType::AddLearnerNode,
            ConfChangeType::BeginMembershipChange,
            ConfChangeType::FinalizeMembershipChange,
        ];
        VALUES
    }
}
