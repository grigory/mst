#include <stdio.h>
#include <iostream>
#include <cstring>

using namespace std;

int main(int argc, char* argv[]) {
	int n, d, mod;
	sscanf(argv[1], "%d", &n);
	sscanf(argv[2], "%d", &d);
	sscanf(argv[3], "%d", &mod);

	freopen("geninput.txt", "w", stdout);

	cout << 3 * n << " " << d << endl;

	srand(566);
	for (int i = 0; i < n; i++) {
		cout << "i " << i << " " << rand() % 2 << " ";
		for (int j = 0; j < d; j++) {
			cout << rand() % mod << " ";
		}
		cout << endl;
	}

	for (int i = 0; i < n; i++) {
		cout << "q" << endl;
		cout << "d " << i << endl;
	}

	return 0;
}
