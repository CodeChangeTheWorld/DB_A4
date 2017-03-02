
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;
	// and the root location
	rootLocation = getTable ()->getRootLocation ();
	if(rootLocation==-1){
		getTable()->setRootLocation(0);
		rootLocation =0;
	}
	shared_ptr <MyDB_PageReaderWriter> rootPage = make_shared <MyDB_PageReaderWriter> (*this, rootLocation);
	rootPage->setType(DirectoryPage);
	rootPage->clear();
	MyDB_INRecordPtr rootNode = getINRecord();
	rootNode->setPtr(1);
	rootPage->append(rootNode);

	shared_ptr <MyDB_PageReaderWriter> leafPage = make_shared <MyDB_PageReaderWriter> (*this, 1);
	leafPage->setType(RegularPage);
}


MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr, MyDB_AttValPtr) {
	return nullptr;
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr, MyDB_AttValPtr) {
	return nullptr;
}


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int, vector <MyDB_PageReaderWriter> &, MyDB_AttValPtr, MyDB_AttValPtr) {
	return false;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr rec) {
	//locate to the page that the record belong

	shared_ptr <MyDB_PageReaderWriter> rootPage = make_shared <MyDB_PageReaderWriter> (*this, rootLocation);
	bool find = false;
	shared_ptr <MyDB_PageReaderWriter> curPage = rootPage;
	MyDB_INRecordPtr recin = getINRecord();
	while(!find){
		MyDB_RecordIteratorAltPtr it=  curPage->getIteratorAlt();
		while(it->advance()){
			it->getCurrent(recin);
			if(recin->getKey()>rec->getAtt(whichAttIsOrdering)){
				find = true;
				break;
			}
		}
	}

	MyDB_RecordPtr newRec = append(recin->getPtr(), rec);
	if(newRec != nullptr){
		MyDB_INRecordPtr newINRec = dynamic_pointer_cast<MyDB_INRecord>(newRec);

		if(curPage->append(newINRec)){
			// sort
			MyDB_RecordPtr lhs = getEmptyRecord();
			MyDB_RecordPtr rhs = getEmptyRecord();
			curPage->sortInPlace(buildComparator(lhs,rhs),lhs,rhs);
		}else{

			// if need new root
			MyDB_INRecordPtr leftINRec = dynamic_pointer_cast<MyDB_INRecord>(split(*curPage,newINRec));
			// build a new root
			MyDB_INRecordPtr rightINRec = getINRecord();
			rightINRec->setPtr(rootLocation);
			int newroot = getTable()->lastPage() + 1;
			getTable()->setLastPage(newroot);
			getTable()->setRootLocation(newroot);
			rootLocation = getTable()->getRootLocation();
			shared_ptr <MyDB_PageReaderWriter> rootPage = make_shared <MyDB_PageReaderWriter> (*this, rootLocation);
			rootPage->setType(DirectoryPage);
			rootPage->clear();
			rootPage->append(leftINRec);
			rootPage->append(rightINRec);
		}

	}

}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter page , MyDB_RecordPtr rec) {
	vector<MyDB_RecordPtr> myvec;
	myvec.push_back(rec);
	MyDB_RecordIteratorAltPtr it =page.getIteratorAlt();
	MyDB_RecordPtr currec = getEmptyRecord();
	while(it->advance()){
		it->getCurrent(currec);
		myvec.push_back(currec);
	}
	MyDB_RecordPtr lhs = getEmptyRecord();
	MyDB_RecordPtr rhs = getEmptyRecord();
	std::stable_sort(myvec.begin(),myvec.end(),buildComparator(lhs,rhs));
	int half = myvec.size()/2;
	page.clear();
	for(int i=half;i<myvec.size();i++){
		page.append(myvec[i]);
	}

	int newpagenum = getTable()->lastPage() + 1;
	getTable()->setLastPage(newpagenum);
	shared_ptr <MyDB_PageReaderWriter> newpage = make_shared <MyDB_PageReaderWriter> (*this, newpagenum);

	for(int i=0;i<half;i++){
		newpage->append(myvec[i]);
	}

	MyDB_INRecordPtr newItem = getINRecord();
	newItem->setKey(myvec[half]->getAtt(whichAttIsOrdering));
	newItem->setPtr(newpagenum);
	return newItem;

}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int page, MyDB_RecordPtr rec) {
	shared_ptr <MyDB_PageReaderWriter> curPage = make_shared <MyDB_PageReaderWriter> (*this, page);
	bool find = true;
	MyDB_INRecordPtr recin = getINRecord();
	if(curPage->getType()==DirectoryPage ){
		MyDB_RecordIteratorAltPtr it =  curPage->getIteratorAlt();
		while(it->advance() && find){
			it->getCurrent(recin);
			if(recin->getKey()>rec->getAtt(whichAttIsOrdering)){
				find = false;
				break;
			}
		}
		MyDB_RecordPtr recptr = append(recin->getPtr(),rec);
		if(recptr == nullptr){
			return nullptr;
		}

		MyDB_INRecordPtr newINRec = dynamic_pointer_cast<MyDB_INRecord>(recptr);
		if(curPage->append(newINRec)){
			// sort
			MyDB_RecordPtr lhs = getEmptyRecord();
			MyDB_RecordPtr rhs = getEmptyRecord();
			curPage->sortInPlace(buildComparator(lhs,rhs),lhs,rhs);
			return nullptr;
		}else{
			return split(*curPage,newINRec);
		}


	}else if(curPage->getType() == RegularPage ){
		if(curPage->append(rec)){
			return nullptr;
		}else{
			return split(*curPage,rec);
		}
	}

	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}


#endif
