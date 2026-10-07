// Every marked line must fail for its stated analyzer code.
// ignore_for_file: unused_local_variable
import '../generated.dart';
import 'package:axton/axton.dart' show Client;

Future<void> misuse(GeneratedClient client, ApplicationTransaction tx, CompanionContext local, Entry row) async {
 Client.open(path:'unused',schema:{},stream:'User:u',projectionGeneration:'2'); // reject: UNDEFINED_NAMED_PARAMETER
 GeneratedClient.open(path:'unused',stream:'User:u',projectionGeneration:'2'); // reject: UNDEFINED_NAMED_PARAMETER
 client.queries.readEntry(id:row.id,once:false); // reject: UNDEFINED_NAMED_PARAMETER
 client.queries.readEntry(id:row.id,refresh:false); // reject: UNDEFINED_NAMED_PARAMETER
 client.queries.invalidate; // reject: UNDEFINED_GETTER
 StoreConnection(url:'unused',token:()=>'x',identity:{}); // reject: UNDEFINED_NAMED_PARAMETER
 client.models.entry.query(where:const EntryFilter(tags:Present(['x']))); // reject: UNDEFINED_NAMED_PARAMETER
 client.models.entry.query(orderBy:[EntryOrder(EntryOrderField.byStatus)]); // reject: UNDEFINED_ENUM_CONSTANT
 client.models.entry.query(where:const EntryFilter(at:Present('2026-01-01'))); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 tx.models.entry.watch(); // reject: UNDEFINED_METHOD
 client.mutate; // reject: UNDEFINED_GETTER
 tx.actions; // reject: UNDEFINED_GETTER
 tx.transaction.mutate; // reject: UNDEFINED_GETTER
 tx.transaction.actions; // reject: UNDEFINED_GETTER
 EntryPatch(id:'bad'); // reject: UNDEFINED_NAMED_PARAMETER
 EditEntryEntryUpdate(identity:EntryIdentity(id:row.id),tags:const Present(['x'])); // reject: UNDEFINED_NAMED_PARAMETER
 const EntryPatch(title:Present(null)); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 Entry(id:row.id,title:row.title,note:row.note,at:row.at,tags:row.tags,status:Status.typo); // reject: UNDEFINED_ENUM_CONSTANT
 const DraftCreate(); // reject: MISSING_REQUIRED_ARGUMENT
 const DraftCreate(memo:null,note:'plain'); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 Draft(body:'x',mood:Mood.calm,created:DateTime.utc(2020),note:null,memo:null); // reject: MISSING_REQUIRED_ARGUMENT
 client.fetch.placement(const PlacementIdentity(shelf:'s')); // reject: MISSING_REQUIRED_ARGUMENT
 client.fetch.placement(PlacementIdentity(shelf:'s',at:'2026-01-01')); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 client.fetch.entry(const EntryIdentity(id:1)); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 client.fetch.entry(row); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 client.fetch.entry(EntryIdentity(id:row.id),store:{'entry':false}); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 client.fetch.entry(EntryIdentity(id:row.id),once:true); // reject: UNDEFINED_NAMED_PARAMETER
 client.fetch.entry(EntryIdentity(id:row.id),refresh:true); // reject: UNDEFINED_NAMED_PARAMETER
 tx.fetch; // reject: UNDEFINED_GETTER
 tx.queries; // reject: UNDEFINED_GETTER
 tx.transaction.fetchModel; // reject: UNDEFINED_GETTER
 tx.mutations.call; // reject: UNDEFINED_GETTER
 client.mutations.rename(const RenameInput(id:'id',title:'t'),store:false); // reject: UNDEFINED_NAMED_PARAMETER
 client.mutations.rename(const RenameInput(id:'id',title:'t'),local:(local){}); // reject: UNDEFINED_NAMED_PARAMETER
 client.mutations.rename.withTransaction((GeneratedTransaction other)async=>const RenameInput(id:'id',title:'t')); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 final Call<String> wrong=await client.mutations.publishEntry(PublishEntryInput(entry:row,composition:row.id)); // reject: INVALID_ASSIGNMENT
 local.mutations; // reject: UNDEFINED_GETTER
 local.streams; // reject: UNDEFINED_GETTER
 local.channels; // reject: UNDEFINED_GETTER
 local.transaction; // reject: UNDEFINED_GETTER
 local.models.composition.watch(); // reject: UNDEFINED_METHOD
 client.streams; // reject: UNDEFINED_GETTER
 client.channels; // reject: UNDEFINED_GETTER
 tx.streams; // reject: UNDEFINED_GETTER
 client.loads; // reject: UNDEFINED_GETTER
 client.client.startLoad; // reject: UNDEFINED_GETTER
 client.mutations.rename(const RenameInput(id:null,title:'t')); // reject: ARGUMENT_TYPE_NOT_ASSIGNABLE
 final DraftFields fields=const DraftCreate(memo:null); // reject: INVALID_ASSIGNMENT
 fields.identity; // reject: UNDEFINED_GETTER
 client.models.draftFields; // reject: UNDEFINED_GETTER
 row.missing; // reject: UNDEFINED_GETTER
 StoreHooks(entry:(tx,changes){}); // reject: UNDEFINED_FUNCTION
 GeneratedClient.open(path:'unused',stream:'U',onStore:{}); // reject: UNDEFINED_NAMED_PARAMETER
}
