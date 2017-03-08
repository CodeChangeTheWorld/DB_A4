
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

}


MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	vector <MyDB_PageReaderWriter> vec;
	discoverPages(rootLocation, vec, low, high);
	MyDB_RecordPtr lhs = getEmptyRecord();
	MyDB_RecordPtr rhs = getEmptyRecord();
	MyDB_RecordPtr rec = getEmptyRecord();
	MyDB_INRecordPtr mylow = getINRecord();
	MyDB_INRecordPtr myhigh = getINRecord();
	mylow->setKey(low);
	myhigh->setKey(high);

	function<bool()> comparator = buildComparator(lhs, rhs);
	function<bool()> lowcomparator = buildComparator(rec, mylow);
	function<bool()> highcomparator = buildComparator(myhigh, rec);


	return make_shared<MyDB_PageListIteratorSelfSortingAlt>(vec, lhs, rhs, comparator, rec, lowcomparator, highcomparator, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {

	vector <MyDB_PageReaderWriter> vec;
	discoverPages(rootLocation, vec, low, high);
	MyDB_RecordPtr lhs = getEmptyRecord();
	MyDB_RecordPtr rhs = getEmptyRecord();
	MyDB_RecordPtr rec = getEmptyRecord();
	MyDB_INRecordPtr mylow = getINRecord();
	MyDB_INRecordPtr myhigh = getINRecord();
	mylow->setKey(low);
	myhigh->setKey(high);

	function<bool()> comparator = buildComparator(lhs, rhs);
	function<bool()> lowcomparator = buildComparator(rec, mylow);
	function<bool()> highcomparator = buildComparator(myhigh, rec);


	return make_shared<MyDB_PageListIteratorSelfSortingAlt>(vec, lhs, rhs, comparator, rec, lowcomparator, highcomparator, false);
}

bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list,
												  MyDB_AttValPtr low, MyDB_AttValPtr high) {
	//if current page is a leaf page add it to list and return.
	MyDB_PageReaderWriter curpage = (*this)[whichPage];
	if(curpage.getType() == RegularPage){
		list.push_back(curpage);
		return true;
	}else{
		MyDB_RecordIteratorAltPtr it = curpage.getIteratorAlt();
		MyDB_INRecordPtr cur = getINRecord();
		MyDB_INRecordPtr highrec = getINRecord();
		MyDB_INRecordPtr lowrec = getINRecord();
		lowrec->setKey(low);
		highrec->setKey(high);
		function<bool()> complow=buildComparator(cur,lowrec);
		function<bool()> comphigh=buildComparator(highrec,cur);
		bool gtlow = false, lthigh = true, resultinchild = false;
		while(it->advance()){
			it->getCurrent(cur);
			//recurse down to find first key
			if(!complow()){
				gtlow=true; //find the first key
			}
			if(gtlow&&lthigh){
				if(resultinchild){
					//add all pages in the path into result;
					list.push_back((*this)[cur->getPtr()]);
				}else{
					//recursively traverse the tree.
					resultinchild = discoverPages(cur->getPtr(),list, low,high);
				}
			}
			//until go out of range, exit
			if(comphigh()){
				lthigh=false;
			}
		}
	}
	return false;
}
//bool MyDB_BPlusTreeReaderWriter :: discoverPages (int, vector <MyDB_PageReaderWriter> &, MyDB_AttValPtr, MyDB_AttValPtr) {
//
//	return false;
//}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr rec) {

	//locate to the page that the record belong
	rootLocation = getTable()->getRootLocation();

	// check if activated
	if(rootLocation == -1){
		getTable()->setRootLocation(0);
		rootLocation = getTable()->getRootLocation();
		getTable()->setLastPage(0);
		shared_ptr <MyDB_PageReaderWriter> rootPage = make_shared <MyDB_PageReaderWriter> (*this, rootLocation);
		rootPage->clear();
		lastPage = rootPage;
		rootPage->setType(DirectoryPage);

		MyDB_INRecordPtr rootNode = getINRecord();
		rootNode->setPtr(1);
		rootPage->append(rootNode);
		getTable()->setLastPage(1);
		shared_ptr <MyDB_PageReaderWriter> leafPage = make_shared <MyDB_PageReaderWriter> (*this, getTable()->lastPage());
		leafPage->clear();
		lastPage = leafPage;
		leafPage->setType(RegularPage);
	}


	shared_ptr <MyDB_PageReaderWriter> rootPage = make_shared <MyDB_PageReaderWriter> (*this, rootLocation);

	MyDB_RecordPtr newRec = append(rootLocation, rec);

	if(newRec != nullptr){
		int oldroot = rootLocation;
		int newroot = getTable()->lastPage() + 1;
		getTable()->setLastPage(newroot);
		getTable()->setRootLocation(newroot);
		rootLocation = getTable()->getRootLocation();
		shared_ptr <MyDB_PageReaderWriter> newrootPage = make_shared <MyDB_PageReaderWriter> (*this, rootLocation);
		newrootPage->clear();
		newrootPage->setType(DirectoryPage);
		newrootPage->append(newRec);
		MyDB_INRecordPtr secRec = getINRecord();
		secRec->setPtr(oldroot);
		newrootPage->append(secRec);

	}

}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter page , MyDB_RecordPtr rec) {


	int newpagenum = getTable()->lastPage() + 1;
//	getTable()->setLastPage(newpagenum);
	MyDB_PageReaderWriter newpage = (*this)[newpagenum];
	newpage.clear();
	MyDB_RecordPtr lhs = getEmptyRecord();
	MyDB_RecordPtr rhs = getEmptyRecord();
	newpage.setType(RegularPage);
	MyDB_RecordPtr currec = getEmptyRecord();
	if(page.getType() == DirectoryPage){
		newpage.setType(DirectoryPage);
		lhs = getINRecord();
		rhs = getINRecord();
		currec = getINRecord();
	}
	function <bool()> comparator = buildComparator(lhs, rhs);
	page.sortInPlace(comparator,lhs,rhs);
	int size = page.getPageSize() - sizeof (size_t) * 2;
	int bytesConsumed = 0;
 	MyDB_RecordIteratorAltPtr it =	page.getIteratorAlt();

	bool added = false;
	function <bool()> innercomparator = buildComparator(rec, currec);
	while(bytesConsumed < size/2 && it->advance()){
		it->getCurrent(currec);
		if(!added && innercomparator()){
			bytesConsumed += rec->getBinarySize();
			bool test = newpage.append(rec);
			if(!test){
				cout<<"233"<<endl;
			}
			added = true;
		}
		bytesConsumed += currec->getBinarySize();
		bool test = newpage.append(currec);
		if(!test){
			cout<<"233333"<<endl;
		}

	}

	MyDB_INRecordPtr newItem = getINRecord();
	newItem->setKey(getKey(currec));
	newItem->setPtr(newpagenum);

	vector<MyDB_RecordPtr> tempvec;
	while(it->advance()){
        MyDB_RecordPtr temprec;
        if(page.getType() == RegularPage) temprec = getEmptyRecord();
        else temprec = getINRecord();
        function <bool()> innercomparator = buildComparator(rec, temprec);
		it->getCurrent(temprec);
		if(!added && innercomparator()){
			bytesConsumed += rec->getBinarySize();
			tempvec.push_back(rec);
			added = true;
		}
		bytesConsumed += currec->getBinarySize();
		tempvec.push_back(temprec);
	}
    if(!added) tempvec.push_back(rec);
	vector<MyDB_RecordPtr>::iterator iter;
	page.clear();
	page.setType(newpage.getType());
	for(iter = tempvec.begin(); iter != tempvec.end(); iter++){
		bool test = page.append(*iter);
		if(!test){
			cout<<"233"<<endl;
		}
	}

	return newItem;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int page, MyDB_RecordPtr rec) {
	MyDB_PageReaderWriter curPage = (*this)[page];
	bool find = true;
	MyDB_INRecordPtr recin = getINRecord();
    function<bool()> comparator = buildComparator(recin, rec);
	if(curPage.getType()==DirectoryPage ){
		MyDB_RecordIteratorAltPtr it = curPage.getIteratorAlt();
		while(it->advance() && find){
			it->getCurrent(recin);
			if(!comparator()){
				find = false;
				break;
			}
		}
		auto recptr = append(recin->getPtr(),rec);
		if(recptr == nullptr){
			return nullptr;
		}

		if(curPage.append(recptr)){
			// sort
			MyDB_INRecordPtr other = getINRecord();
			function <bool()> comparator = buildComparator(recptr, other);
			curPage.sortInPlace(comparator,recptr,other);
			return nullptr;
		}else{
			return split(curPage,recptr);
		}


	}else if(curPage.getType() == RegularPage ){
		if(curPage.append(rec)){
			return nullptr;
		}else{
			return split(curPage,rec);
		}
	}

	return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
	printTree(rootLocation,0);
}

void  MyDB_BPlusTreeReaderWriter::printTree(int page, int level) {
	//if it is an internal page, recursively print it's children and print it self
	MyDB_PageReaderWriter curPage = (*this)[page];
	if(curPage.getType() == DirectoryPage){
		MyDB_RecordIteratorAltPtr it = curPage.getIteratorAlt();
		MyDB_INRecordPtr rec = getINRecord();
		while(it->advance()){
			it->getCurrent(rec);
			//print children
			printTree(rec->getPtr(),level+1);
			//find the right place
			int tmp = level;
			while(tmp>=0){
				cout<<'\t';
				tmp--;
			}
			//print self
			cout << (MyDB_RecordPtr) rec << endl;
		}
	}else if(curPage.getType() == RegularPage){
		MyDB_RecordIteratorAltPtr it = curPage.getIteratorAlt();
		MyDB_RecordPtr rec = getEmptyRecord();
		while(it->advance()){
			it->getCurrent(rec);
			int tmp = level;
			while(tmp>=0){
				cout<<'\t';
				tmp--;
			}
			//print self
			cout << rec << endl;
	}
	// if it is leaf, print it out
	}
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
