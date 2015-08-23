#include <assert.h>
#include <iostream>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <string>
#include <sstream>
#include <vector>
#include <queue>
#include <functional>
#include <map>
#include <set>

#define sq(x) (((double) (x))*((double) (x)))
#define MAX (1<<29)
#define INF (1e100)

using namespace std;

typedef int *Point;
int d;

map<int, pair<Point, int> > points;

struct item {
	Point key;
	int prior, cnt;
	item * l, * r;
	item() { }
	item (Point key, int prior) : key(key), prior(prior), cnt(0), l(NULL), r(NULL) { }
};

typedef item * pitem;


void printout(Point p) {
	for (int i = 0; i < d; i++) {
		cout << p[i] << " ";
	}
	cout << endl;
}

double dist_sq(const Point p, const Point q) {
  	double ret = 0;
	for (int j = 0; j < d; j++) ret += sq(p[j] - q[j]);
	return ret;
}

class Edge {
	public:
	Point p1, p2;
	
	Edge(Point tp1 = NULL, Point tp2 = NULL) {
		p1 = tp1;
		p2 = tp2;
		if (p1 == NULL) {
			p1 = new int[d];
		}
		if (p2 == NULL) {
			p2 = new int[d];
		}
	}

	bool operator<(const Edge & e) const {
		if (fabs(dist_sq(p1, p2) - dist_sq(e.p1, e.p2)) > 1e-6) {
			return dist_sq(p1, p2) < dist_sq(e.p1, e.p2);
		}
		return ((p1 < e.p1) || ((p1 == e.p1) && (p2 < e.p2)));
	}

	bool operator==(const Edge & e) const {
		return ((p1 == e.p1) && (p2 == e.p2));
	}
	
	void printout() const {
		cout << "p1 ";
		for (int i = 0; i < d; i++) {
			cout << p1[i] << " ";
		}
		cout << "p2 ";
		for (int i = 0; i < d; i++) {
			cout << p2[i] << " ";
		}
		cout << endl;
	}
};

Edge found;


struct ANN {
	pitem t;
	Point ans, q1, q2;
	int shift;
	double eps, r, r_sq;
	int n;

	ANN(pitem tt = NULL, int tn = 0, Point tq1 = NULL, Point tq2 = NULL, int tshift = 0, double teps = 0, Point tans = NULL, double tr = 0, double tr_sq = INF) {
		t = tt;
		n = tn;
		ans = tans;
		eps = teps;
		r = tr;
		r_sq = tr_sq;
		shift =  (int) (drand48()*MAX); 
		q1 = new int[d]; 
		q2 = new int[d];
	}

	void clear() {
	/*	if (t != NULL) delete t;
		if (ans != NULL) delete ans;
		if (q1 != NULL) delete q1;
		if (q2 != NULL) delete q2;
	*/
	}
};


void print(Point p) {
	for (int i = 0; i < d; i++) {
		//cerr << p[i] << " ";
	}
}


void printoutmap(map<Point, Point> & m) {
	map<Point, Point> inverse;
	cout << "Printing map";
	for (map<Point, Point>::iterator it = m.begin(); it != m.end(); it++) {
		printout((*it).first);
		printout((*it).second);
		if (inverse.count((*it).second)) {
			cout << "Two things map to one" << endl;
			cout << inverse[(*it).second] << endl;
			cout << (*it).first<< endl;
			assert(false);
		}
		inverse[(*it).second] = (*it).first;
	}
	cout << "Done" << endl;
}

void printoutmap(map<Edge, int> & m) {
	cout << "Printing map";
	for (map<Edge, int>::iterator it = m.begin(); it != m.end(); it++) {
		((*it).first).printout();
		cout << (*it).second;
	}
	cout << "Done" << endl;
	
}

void printoutset(set<Point> & s) {
	for (set<Point>::iterator it = s.begin(); it != s.end(); it++) {
		printout(*it);
	}
}

inline int less_msb(int x, int y) { return x < y && x < (x^y); }

int cmp_shuffle(ANN & nns, Point *p, Point *q) {
  int j, k, x, y;
  for (j = k = x = 0; k < d; k++)
    if (less_msb(x, y = ((*p)[k]+nns.shift)^((*q)[k]+nns.shift))) {
      j = k; x = y;
    }
  if ((*p)[j] != (*q)[j]) {
  	return ((*p)[j] - (*q)[j]);
  }
  return ((*p) - (*q));
}


int cnt (pitem t) {
	return t ? t->cnt : 0;
}

void upd_cnt (pitem t) {
	if (t)
		t->cnt = 1 + cnt(t->l) + cnt (t->r);
}

void split (ANN & nns, pitem t, Point key, pitem & l, pitem & r) {
	if (!t)
		l = r = NULL;
	else if (cmp_shuffle(nns, &key, &t->key) < 0)
		split (nns, t->l, key, l, t->l),  r = t;
	else
		split (nns, t->r, key, t->r, r),  l = t;
	upd_cnt(t);
}

void merge (pitem & t, pitem l, pitem r) {
	if (!l || !r)
		t = l ? l : r;
	else if (l->prior > r->prior)
		merge (l->r, l->r, r),  t = l;
	else
		merge (r->l, l, r->l),  t = r;
	upd_cnt(t);	
}

void insert (ANN & nns, pitem & t, pitem it) {
	if (!t)
		t = it;
	else if (it->prior > t->prior)
		split (nns, t, it->key, it->l, it->r),  t = it;
	else
		insert (nns, cmp_shuffle(nns, &it->key, &t->key) < 0 ? t->l : t->r, it);
	upd_cnt(t);
}

void printcomparison(ANN & nns, Point p, pitem t) {
	if (!t) {
		return;
	}
	cout << "Printing comparison" << endl;
	printout(p);
	printout(t->key);
	cout << cmp_shuffle(nns, &p, &t->key) << endl;
}

void printtree(ANN & nns, pitem t, int level) {
	if (!t) {
		return;
	}
	for (int i = 0; i < level; i++) {
		cout<< " "; 
	}
	printout(t->key);
	cout << " " << t->prior;
	cout << endl;
//	cout << "Left comparison" << endl;
//	printcomparison(nns, t->key, t->l);
//	cout << "Right comparison" << endl;
//	printcomparison(nns, t->key, t->r);
	printtree(nns, t->l, level + 1);
	printtree(nns, t->r, level + 1);
}

void erase (ANN & nns, pitem & t, Point key) {
	if (t == 0) {
		return;
	}
	if (cmp_shuffle(nns, &(t->key), &key) == 0) {
		merge (t, t->l, t->r);
		nns.n--;
	}
	else { 
		erase (nns, cmp_shuffle(nns, &key, &(t->key)) < 0 ? t->l : t->r, key);
	}
	upd_cnt(t);
}


pitem get(pitem & t, int k) {
	if (k >= cnt(t)) {
		return NULL;
	}
	int left = cnt(t->l);
	if (k < left) {
		return get(t->l, k);
	} else if (k == left) {
		return t;
	} else {
		return get(t->r, k - 1 - left);
	}
}


void check_dist(ANN & nns, Point p, Point q) {
  double z = dist_sq(p, q);
  if (z < nns.r_sq) {
    nns.r_sq = z; nns.r = sqrt(z); nns.ans = p;
    for (int j = 0; j < d; j++) {
      nns.q1[j] = (q[j] > nns.r) ? (q[j] - (int)ceil(nns.r)) : 0; 
      nns.q2[j] = (q[j] + nns.r < MAX) ? (q[j]+(int)ceil(nns.r)) : MAX;
    }
  }
}

double dist_sq_to_box(ANN & nns, Point q, Point p1, Point p2) {
  int i, j, x, y; double z;
  for (j = x = 0; j < d; j++)
    if (less_msb(x, y = (p1[j] + nns.shift)^(p2[j] + nns.shift))) x = y;
  frexp(x, &i);
  for (j = 0, z = 0; j < d; j++) {
    x = ((p1[j] + nns.shift) >> i) << i; y = x + (1 << i);
    if (q[j] + nns.shift < x) z += sq(q[j] + nns.shift - x);
    else if (q[j] + nns.shift > y) z += sq(q[j] + nns.shift - y);
  }
  return z;
}

void SSS_insert(ANN & nns, Point p) {
	nns.n++;
	insert(nns, nns.t, new item(p, rand()));
}

void SSS_erase(ANN & nns, Point key) {
	erase(nns, nns.t, key);
}

void SSS_query0(ANN & nns, int shift, int n, Point q) {
  if (n == 0) return;
  check_dist(nns, get(nns.t, shift + n / 2)->key, q);
  if (n == 1 || dist_sq_to_box(nns, q, get(nns.t, shift)->key, get(nns.t, shift + n - 1)->key)*sq(1 + nns.eps) > nns.r_sq) return;
  if (cmp_shuffle(nns, &q, &(get(nns.t, shift + n / 2)->key)) < 0) {
    SSS_query0(nns, shift, n / 2, q);
    if (cmp_shuffle(nns, &nns.q2, &(get(nns.t, shift + n / 2)->key)) > 0) SSS_query0(nns, shift + n / 2 + 1, n - n / 2 - 1, q);
  }
  else {
    SSS_query0(nns, shift + n / 2 + 1, n - n / 2 - 1, q);
    if (cmp_shuffle(nns, &nns.q1, &(get(nns.t, shift + n / 2)->key)) < 0) SSS_query0(nns, shift, n / 2, q);
  }
}

Point SSS_query(ANN & nns, Point q) {
  nns.r_sq = INF; 
  SSS_query0(nns, 0, nns.n, q); 
  return nns.ans;
}

void SSS_build(ANN & nns, vector<Point> & v) {
	for (int i = 0; i < v.size(); i++) {
		SSS_insert(nns, v[i]);
	}
}

template <class T> class heap {
private:	
	vector<T> a;
	int n;
	map<T, int> positions;
public:
	heap(int capacity = 0, int tn = 0) {
		a.reserve(capacity);
		a.resize(capacity);
		n = tn;
	}

	void printout() {
		for (int i = 0; i< n; i++) {
			a[i].printout();
		}
	}

	void clear() {
		a.clear();
		n = 0;
		positions.clear();
	}

	inline int parent(int pos) {
		return (pos - 1)/ 2;
	}

	inline int left (int pos) {
		return 2 * pos + 1;
	}
	
	inline int right(int pos) {
		return 2 * pos + 2;
	}

	inline int size() {
		return n;
	}

	void check() {
		//cout << positions.size() << " " << n << endl;
		//assert(positions.size() == n);
	/*	if (positions.count(found)) {
			assert(positions.count(a[positions[found]]));
			cout << "Found is ";
			found.printout();
			cout << endl;
			printoutmap(positions);
			cout << "Found position is = " << positions[found] << " while at position ";
			a[positions[found]].printout();
			cout << " whose position is " << positions[a[positions[found]]];
			cout << " n = " << n << endl;
			assert(a[positions[found]] == found);
		}*/
	/*	for (int i = 0; i < n; i++) {
			assert(positions[a[i]] == i);
			if (a[i].p1[0] == 9 && a[i].p1[1] == 6 && a[i].p2[0] == 6 && a[i].p2[1] == 9) {
				cout << "Found at position " << i << endl; 
				found = a[i];
			}
		}
		cout << "Queue: ";
		printout();*/
	
	}

	void myswap(int p1, int p2) {
		//cout << "myswap";
		//check();
		positions[a[p1]] = p2;
		positions[a[p2]] = p1;
		swap(a[p1], a[p2]);
		//check();
	}

	void siftup(int pos) {
		//cout << "siftup" << endl;
		//check();
		while (pos && (a[pos] < a[parent(pos)])) {
			myswap(parent(pos), pos);
			pos = parent(pos);
		}
		//check();
	}

	void siftdown(int pos) {
		//cout << "siftdown" << endl;
		//check();
		while (left(pos) < size()) {
			if (right(pos) < size()) {
				if ((a[right(pos)] < a[pos]) && (a[right(pos)] < a[left(pos)])) {
					myswap(right(pos), pos);
					pos = right(pos);
					continue;
				}
			} 
			if (a[left(pos)] < a[pos]) {
				myswap(left(pos), pos); 
				pos = left(pos);
			} else {
				break;
			}
		}
	//	check();
	}

	void insert(T key) {
	//	cout << "insert" << endl;
	//	check();
	//	cout << "Inserting ";
	//	key.printout(); 
	//	cout << endl;
	///	cout << "Queue: ";
	//	printout();
		if (positions.count(key)) {
	//		cout << "Already present at position " << positions[key] <<  endl;
	//		if (!(a[positions[key]] == key)) {
	//			cout << "ERROR: Key = ";
	//			key.printout();
	//			cout << " while at position is ";
	//			a[positions[key]].printout();
	//			cout << endl;
//
//			}
//			check();
			return;
		}
		if (n >= a.size()) {
			a.resize(2 * n + 1);
		}
		a[n] = key;
		positions[key] = n;
		n++;
		siftup(n - 1);
//		check();
	}

	T top() {
//		cout << "top" << endl;
//		cout << size() << endl;
//		check();
		assert(size() > 0);
		return a[0];
	}
	
	void erasetop() {
		n--;
		positions.erase(a[0]);
		if (!n) {
			return;
		}
		a[0] = a[n];
		positions[a[0]] = 0;
		siftdown(0);
	}

	void erase(T key) {
//		check();
//		cout << "Erasing ";
//		key.printout();
//		cout << endl;
		if (!positions.count(key)) {
//			check();
			return;
		}
//		cout << "Count = " << positions.count(key) << endl;
//		cout << "Position = " << positions[key] << endl;
		int pos = positions[key];
//		cout << positions.size() << " " << n  << endl;
		positions.erase(key);
//		cout << positions.size() << endl;
		n--;
		if (pos == n) {
//			check();
			return;
		}
		a[pos] = a[n];
		positions[a[pos]] = pos;
		if (pos && (key < a[parent(pos)])) {
			siftup(pos);
		} else {
			siftdown(pos);
		}
//		check();
	}
};

void addNNSEdge(ANN & nns, Point & cur, map<Point, Point> & out, map<Point, Point> & in, heap<Edge> & pq) {
	Point next = SSS_query(nns, cur);
	out[cur] = next;
	in[next] = cur;
	pq.insert(Edge(cur, next));
	cur = next;
	int prev = nns.n;
	SSS_erase(nns, cur);
//	cout << "Point deleted from nns";
//	printout(cur);
//	printtree(nns, nns.t, 0);
//	assert(nns.n == prev - 1);
}

struct List {
	map<Point, Point> outEdge;
	map<Point, Point> inEdge;

	ANN nnsleft, nnsright;

	List() {
	}

	List(vector<Point> & left, set<Point> & sright, heap<Edge> & pq) {
		SSS_build(nnsleft, left);
		vector<Point> right = vector<Point>(sright.begin(), sright.end());
		SSS_build(nnsright, right);
		if (left.size() == 0) {
			return;
		}
		//cerr << "HERE" << endl;
		Point cur = left[0];
		SSS_erase(nnsleft, cur);
		while (1) {
//			cout << "p1" << endl;
			if (!nnsright.n) {
				break;
			}
//			cout << "p2" << endl;
			addNNSEdge(nnsright, cur, outEdge, inEdge, pq);
//			cout << "p3" << endl;
//			cout << "nnsleft.n "<< nnsleft.n << endl;
			if (!nnsleft.n) {
				break;
			}
//			cout << "p4" << endl;
			addNNSEdge(nnsleft, cur, outEdge, inEdge, pq);	
//			cout << "p5" << endl;
		}
	}

	void printout() {
		cout << "outEdge ";
		printoutmap(outEdge);
		cout << "inEdge ";
		printoutmap(inEdge);

	}

	void eraseFromList(Point p, vector<Point> & v, vector<Point> & pqremove) {
//		cout << "We are erasing here ";
//		::printout(p);
//		cout << endl;
		
//		cout << "OutEdge map" << endl;
//		printoutmap(outEdge);
//		cout << "InEdge map" << endl;
//		printoutmap(inEdge);

		if (outEdge.count(p)) {
//			cout << "erased outedge" << endl;
			Point other = outEdge[p];
			outEdge.erase(p);
			inEdge.erase(other);
			pqremove.push_back(other);
		}
		if (inEdge.count(p)) {
//			cout << "erased inedge" << endl;
			Point erased = inEdge[p];
			v.push_back(erased);
			inEdge.erase(p);
			outEdge.erase(erased);
			pqremove.push_back(erased);
		}
	}

	void eraseFromListNNS(Point p) {
		SSS_erase(nnsleft, p);
		SSS_erase(nnsright, p);
	}

	void clear() {
		outEdge.clear();
		inEdge.clear();
		nnsleft.clear();
		nnsright.clear();
	}
};



int mylog(int k) {
	int ret = 0;
	while (k) {
		k >>= 1;
		ret++;
	}
	return ret;
}

struct Level {
	List lists[2];
	heap<Edge> pq;
	set<Point> points[2];

	Level() {
	}


	Level(vector <Point> l[2], set<Point> spoints[2]) {	
		points[0] = set<Point>(l[0].begin(), l[0].end());
		points[1] = set<Point>(l[1].begin(), l[1].end());
//		cout << "Consructing lsit 0" << endl;
		lists[0] = List(l[0], spoints[1], pq);
//		cout << "Constructing list 1" << endl;
		lists[1] = List(l[1], spoints[0], pq);
//		cerr << "Done constructing lists" << endl;
	}
	
	Level(set<Point> sl[2], set<Point> spoints[2]) {
		vector<Point> l[2];
		for (int i = 0; i < 2; i++) {
			l[i] = vector<Point>(sl[i].begin(), sl[i].end());
		}
		points[0] = set<Point>(l[0].begin(), l[0].end());
		points[1] = set<Point>(l[1].begin(), l[1].end());
		lists[0] = List(l[0], spoints[1], pq);
		lists[1] = List(l[1], spoints[0], pq); 
	}

	void printout() {
		for (int i = 0; i < 2; i++) {
			cout << "list " << i << " ";
			lists[i].printout();
		}
		cout << "pq ";
		pq.printout();
		cout << "s0 ";
		printoutset(points[0]);
		cout << "s1 ";
		printoutset(points[1]);

	}

	double getmin() {
		if (pq.size() == 0) {
			return INF;
		}
		Edge etop = pq.top();
//		etop.printout();
		return dist_sq(etop.p1, etop.p2);
	}

	int npoints() {
		return points[0].size() + points[1].size();
	}

	void eraseFromLevelNNS(Point p) {
		for (int i = 0; i < 2; i++) {
			lists[i].eraseFromListNNS(p);
		}
	}

	void erase(Point p, int color, vector<Point> & ret) {
		points[color].erase(p);
		vector<Point> pqremove;
		for (int i = 0; i < 2; i++) {
//			cout << "ERASING FROM LIST " << i << endl;
			lists[i].eraseFromList(p, ret, pqremove);
		}
		for (int i = 0; i < ret.size(); i++) {
			points[1 - color].erase(ret[i]);
//			cout << "Erasing from list ";
//			::printout(ret[i]);
//			cout << endl;
		}
		for (int i = 0; i < pqremove.size(); i++) {
			pq.erase(Edge(p, pqremove[i]));
			pq.erase(Edge(pqremove[i], p));
		}
	}

	void clear() {
		for (int i = 0; i < 2; i++) {
			points[i].clear();
			lists[i].clear();
		}
		pq.clear();
	}
};

void vectormerge(vector<Point> & a, vector<Point> & b, vector<Point> & ab) {
	ab.reserve(a.size() + b.size());
	ab.insert(ab.end(), a.begin(), a.end());
	ab.insert(ab.end(), b.begin(), b.end());
}

struct ApproxBiNN {
	vector<Level> levels;
	int nlevels;
	set<Point> points[2];

	ApproxBiNN() {
		nlevels = 1;
		levels.resize(nlevels);
		levels[0] = Level();
	}

	ApproxBiNN(vector<Point> l[2]) {
		int n = l[0].size() + l[1].size();
		for (int i = 0; i < 2; i++) {
			points[i] = set<Point>(l[i].begin(), l[i].end());
		}
		nlevels = mylog(n) + 1; 
		levels.resize(nlevels);
		levels[nlevels - 1] = Level(l, points);
	}

	void printout() {
		assert(nlevels == levels.size());
		for (int i = 0; i < nlevels; i++) {
			cout << "LEVEL " << i;
			levels[i].printout();
		}
		printoutset(points[1]);
	}

	void uplevel(Level & low, Level & high) {
		vector<Point> tpoints[2];
		for (int i = 0; i < 2; i++) {
			vector<Point> v1(low.points[i].begin(), low.points[i].end());
			vector<Point> v2(high.points[i].begin(), high.points[i].end());
			vectormerge(v1, v2, tpoints[i]);
		}
		high = Level(tpoints, points);
	}

	void normalize() {
		int last = 0;
		while (last < nlevels - 1) {
			if (levels[last].npoints() > (1 << last)) {
				uplevel(levels[last], levels[last + 1]);
			} else {
				break;
			}
			last++;
		}
		if (levels[nlevels - 1].npoints() > (1 << (nlevels - 1))) {
			nlevels++;
			levels.resize(nlevels);
			levels[nlevels - 1] = Level();
		//	cout << "CONSTRUCTING LEVEL " << nlevels - 1 << endl; 
			uplevel(levels[nlevels - 2], levels[nlevels - 1]);
			last = nlevels - 1;
		}
		for (int i = 0; i < last; i++) {
			levels[i].clear();
		}
//		cout << "CONSTRUCTING LEVEL " << last << endl;
		levels[last] = Level(levels[last].points, points);
	}

	void insert(Point p, int color) {
		points[color].insert(p);
		//cerr << "p1" << endl;
		levels[0].points[color].insert(p);
		//cerr << "p2" << endl;
		normalize();
		//cerr << "p3" << endl;
	}


	void erase(Point p, int color) {
		points[color].erase(p);
		vector<Point> erased;
		for (int i = 0; i < nlevels; i++) {
//			cout << "ERASING FROM LEVEL " << i << endl;
			levels[i].erase(p, color, erased);
		}
//		cout << "DONE ERASING" << endl;
		
//		printout();

//		cout<< "DONE PRINTING" << endl;

//		cout << "Erasing edges for ";
//		::printout(p);
//		cout << endl;
//		cout << "Erased edges";
		for (int i = 0; i < erased.size(); i++) {
//			::printout(erased[i]);
			levels[0].points[1 - color].insert(erased[i]);
		}
		for (int i = 0; i < levels.size(); i++) {
			levels[i].eraseFromLevelNNS(p);
		}
		normalize();
//		cout << "After normalization" << endl;
//		printout();
	}

	double query () {
		double ret = INF;
		for (int i = 0; i < nlevels; i++) {
			ret = min(ret, levels[i].getmin());
		}
		return ret;
	}
};

map<int, Point> ppoints[2];

double naivequery() {
	double ret = INF;
	for (map<int, Point>::iterator it0 = ppoints[0].begin(); it0 != ppoints[0].end(); it0++) {
		for (map<int, Point>::iterator it1 = ppoints[1].begin(); it1 != ppoints[1].end(); it1++) {
			ret = min(ret, dist_sq((*it0).second, (*it1).second));
		}
	}
	return ret;
}

void naiveinsert(int id, Point p, int color) {
	ppoints[color][id] = p;
}

void naiveerase(int id, int color) {
	ppoints[color].erase(id);
}


int main(int argc, char *argv[]) {
  int n;
  char s[100];	

  bool naive = true;

  if (argc > 1 && !strcmp(argv[1], "n")) {
  	naive = false;
  }

  srand48(566);
  srand(566);

  ApproxBiNN abinn;

  scanf("%d %d\n", &n, &d);
  for (int i = 0; i < n; i++) {
  	char c;
	gets(s);
	istringstream ss(s);
	ss >> c;
	cerr << c << endl;
	cout << c << endl;
	if (c == 'q') {
//		abinn.printout();
		cout << "Querying..." << endl;
		double qanswer = abinn.query();
		cout << " answer = " << qanswer << endl;
		if (naive) {
			cout << " naive = " << naivequery() << endl;
			assert(fabs(qanswer - naivequery()) < 1e-6);
		}
		continue;
	}
	int id;
	ss >> id;
	if (c == 'i') {
		int color;
		ss >> color;
		Point p = new int[d];
		cout << "Point inserted: ";
		for (int j = 0; j < d; j++) {
			ss >> p[j];
		}
//		printout(p);
//		abinn.printout();
		points[id] = make_pair(p, color);
		abinn.insert(p, color);	
		if (naive) {
			naiveinsert(id, p, color);
		}
	} else if (c == 'd') {
		cout << "Point deleted: " << id << endl;
//		abinn.printout();
		pair<Point, int> pp = points[id];
		abinn.erase(pp.first, pp.second);
		if (naive) {
			naiveerase(id, pp.second);
		}
	} else {
		assert(false);
	}
  }
  
  
  /* for (int i = 0; i < n; i++) {
  	char c;
	gets(s);
	istringstream ss(s);
	ss >> c;
	int key;
	ss >> key;
	if (c == 'i') {
		h.insert(key);
	} else if (c == 't') {
		err << h.top() << endl;
	} else if (c == 'd') {
		h.erase(key);
	} else if (c == 'r') {
		h.erasetop();
	}
  }
  */
  
  /* 
   scanf("%d %d\n", &n, &d);
   for (int i = 0; i < n; i++) {
  	char c;
	Point p = new int[d];
	gets(s);	
	istringstream ss(s);
	ss >> c;
	for (int j = 0; j < d; j++) {
  		ss >> p[j];	
	}
	if (c == 'i') {
		insert(t, new item(p, rand()));
	} else if (c == 'd') {
		erase(t, p); 
	} else if (c == 'q') {
		SSS_query(t, cnt(t), p);
		//cerr << r << endl;
	}
  }*/
  return 0;
}
