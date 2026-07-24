# Third-party notices

## gosax

fastsax is a ground-up rewrite of the ideas in
[gosax](https://github.com/orisano/gosax) by Nao Yonashiro, and some code is
directly derived from gosax sources:

- `helpers.go` — `Name`, `NextAttribute`, and `Unescape` (with the shared
  whitespace table and the `CData`/`Comment` trim helpers) are derived from
  gosax's implementations, with local modifications.
- `scan.go` — `scanMarkup`'s markup dispatch (comment/CDATA/DOCTYPE/PI
  handling, including the DOCTYPE bracket counting and the lenient `CDATA[`
  spelling) closely follows gosax's markup state handling, rewritten for
  fastsax's windowless scanner.
- `bench_test.go` — `countAfricaGosax` and the `countAfrica` benchmark
  workload are translated from gosax's `bench_test.go`.

These portions are used under the BSD 2-Clause License:

```
Copyright (c) 2024, Nao Yonashiro
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

fastsax's I/O layer (`source.go`, `prefetch.go`) is an independent design and
does not derive from gosax's `reader.go` or its pkg/json ancestry.
