#include <stdexcept>
#include <type_traits>

namespace butil {
struct place_t { };
constexpr place_t place { };
/* this is the default 'false' case */
template <class T, bool = ::std::is_trivially_destructible<T>::value>
struct storage {
  using value_type = T;
  static constexpr bool nothrow_mv_ctor = ::std::is_nothrow_move_constructible<
    value_type
  >::value;

  union {
    ::std::uint8_t dummy;
    value_type val;
  };
  bool engaged { false };

  constexpr storage () noexcept : dummy { '\0' } { }
  storage (storage const& that) :
    engaged { that.engaged }
  {
    if (not this->engaged) { return; }
    ::new(::std::addressof(this->val)) value_type { that.val };
  }

  storage (storage&& that) noexcept(nothrow_mv_ctor) :
    engaged { that.engaged }
  {
    if (not this->engaged) { return; }
    ::new(::std::addressof(this->val)) value_type { std::move(that.val) };
  }

  constexpr storage (value_type const& value) :
    val { value },
    engaged { true }
  { }

  constexpr storage (value_type&& value) noexcept(nothrow_mv_ctor) :
    val { std::move(value) },
    engaged { true }
  { }

  template <class... Args>
  constexpr explicit storage (place_t, Args&&... args) :
    val { std::forward<Args>(args)... },
    engaged { true }
  { }

  ~storage () noexcept { if (this->engaged) { this->val.~value_type(); } }
};

template <class T>
struct storage<T, true> {
  using value_type = T;
  static constexpr bool nothrow_mv_ctor = ::std::is_nothrow_move_constructible<
    value_type
  >::value;
  union {
    ::std::uint8_t dummy;
    value_type val;
  };
  bool engaged { false };

  constexpr storage () noexcept : dummy { '\0' } { }
  storage (storage const& that) :
    engaged { that.engaged }
  {
    if (not this->engaged) { return; }
    ::new(::std::addressof(this->val)) value_type { that.val };
  }

  storage (storage&& that) noexcept(nothrow_mv_ctor) :
    engaged { that.engaged }
  {
    if (not this->engaged) { return; }
    ::new(::std::addressof(this->val)) value_type {
      std::move(that.val)
    };
  }

  constexpr storage (value_type const& value) :
    val { value },
    engaged { true }
  { }

  constexpr storage (value_type&& value) noexcept(nothrow_mv_ctor) :
    val { ::std::move(value) },
    engaged { true }
  { }

  template <class... Args>
  constexpr explicit storage (place_t, Args&&... args) :
    val { ::std::forward<Args>(args)... },
    engaged { true }
  { }
};

struct in_place_t { };
struct nullopt_t { constexpr explicit nullopt_t (int) noexcept { } };

constexpr in_place_t in_place { };
constexpr nullopt_t nullopt { 0 };

struct bad_optional_access final : ::std::logic_error {
  using ::std::logic_error::logic_error;
};

struct bad_expected_type : ::std::logic_error {
  using ::std::logic_error::logic_error;
};

struct bad_result_condition : ::std::logic_error {
  using ::std::logic_error::logic_error;
};

template<typename T>
struct decay_t {
  using type = typename ::std::decay<T>::type;
};

template<bool T>
struct enable_if_t {
    using type = typename ::std::enable_if<T>::type;
};

/* all-traits */
template <class...> struct all_traits;
template <class T, class... Args>
struct all_traits<T, Args...> : ::std::integral_constant<bool,
  T::value and all_traits<Args...>::value
> { };
template <> struct all_traits<> : ::std::true_type { };

/* any-traits */
template <class...> struct any_traits;
template <class T, class... Args>
struct any_traits<T, Args...> : ::std::integral_constant<bool,
  T::value or any_traits<Args...>::value
> { };
template <> struct any_traits<> : ::std::false_type { };

/* no-traits */
template <class... Args> struct no_traits : ::std::integral_constant<bool,
  not all_traits<Args...>::value
> { };

template <class Type>
struct optional final : private storage<Type> {
  using base = storage<Type>;
  using value_type = typename storage<Type>::value_type;

  /* compiler enforcement */
  static_assert(
    not ::std::is_reference<value_type>::value,
    "Cannot have optional reference (ill-formed)"
  );

  static_assert(
    not ::std::is_same<decay_t<value_type>, nullopt_t>::value,
    "Cannot have optional<nullopt_t> (ill-formed)"
  );

  static_assert(
    not ::std::is_same<decay_t<value_type>, in_place_t>::value,
    "Cannot have optional<in_place_t> (ill-formed)"
  );

  static_assert(
    not ::std::is_same<decay_t<value_type>, ::std::nullptr_t>::value,
    "Cannot have optional nullptr (tautological)"
  );

  static_assert(
    not ::std::is_same<decay_t<value_type>, void>::value,
    "Cannot have optional<void> (ill-formed)"
  );

  static_assert(
    ::std::is_object<value_type>::value,
    "Cannot have optional with a non-object type (undefined behavior)"
  );

  static_assert(
    ::std::is_nothrow_destructible<value_type>::value,
    "Cannot have optional with non-noexcept destructible (undefined behavior)"
  );

  constexpr optional () noexcept { }
  optional (optional const&) = default;
  optional (optional&&) = default;
  ~optional () = default;

  constexpr optional (nullopt_t) noexcept { }

  constexpr optional (value_type const& value) :
    base { value }
  { }

  constexpr optional (value_type&& value) noexcept(base::nothrow_mv_ctor) :
    base { ::std::move(value) }
  { }

  template <
    class... Args,
    class=enable_if_t< ::std::is_constructible<value_type, Args...>::value>
  > constexpr explicit optional (in_place_t, Args&&... args) :
    base { place, ::std::forward<Args>(args)... }
  { }

  template <
    class T,
    class... Args,
    class=enable_if_t<
      ::std::is_constructible<
        value_type,
        ::std::initializer_list<T>&,
        Args...
      >::value
    >
  > constexpr explicit optional (
    in_place_t,
    ::std::initializer_list<T> il,
    Args&&... args
  ) : base { place, il, std::forward<Args>(args)... } { }

  optional& operator = (optional const& that) {
    optional { that }.swap(*this);
    return *this;
  }

  optional& operator = (optional&& that) noexcept (
    all_traits<
      ::std::is_nothrow_move_assignable<value_type>,
      ::std::is_nothrow_move_constructible<value_type>
    >::value
  ) {
    optional { ::std::move(that) }.swap(*this);
    return *this;
  }

  template <
    class T,
    class=enable_if_t<
      not ::std::is_same<decay_t<T>, optional>::value and
      ::std::is_constructible<value_type, T>::value and
      ::std::is_assignable<value_type&, T>::value
    >
  > optional& operator = (T&& value) {
    if (not this->engaged) { this->emplace(::std::forward<T>(value)); }
    else { **this = ::std::forward<T>(value); }
    return *this;
  }

  optional& operator = (nullopt_t) noexcept {
    if (this->engaged) {
      this->val.~value_type();
      this->engaged = false;
    }
    return *this;
  }

  void swap (optional& that) noexcept(
    all_traits<
      std::__is_nothrow_swappable<value_type>,
      ::std::is_nothrow_move_constructible<value_type>
    >::value
  ) {
    using ::std::swap;
    if (not *this and not that) { return; }
    if (*this and that) {
      swap(**this, *that);
      return;
    }

    auto& to_disengage = *this ? *this : that;
    auto& to_engage = *this ? that : *this;

    to_engage.emplace(::std::move(*to_disengage));
    to_disengage = nullopt;
  }

  constexpr explicit operator bool () const { return this->engaged; }

  constexpr value_type const& operator * () const noexcept {
    return this->val;
  }

  value_type& operator * () noexcept { return this->val; }

  value_type const* operator -> () const noexcept {
    return ::std::addressof(this->val);
  }

  value_type* operator -> () noexcept { return ::std::addressof(this->val); }

  template <class T, class... Args>
  void emplace (::std::initializer_list<T> il, Args&&... args) {
    *this = nullopt;
    ::new(::std::addressof(this->val)) value_type {
      il,
      ::std::forward<Args>(args)...
    };
    this->engaged = true;
  }

  template <class... Args>
  void emplace (Args&&... args) {
    *this = nullopt;
    ::new(::std::addressof(this->val)) value_type {
      ::std::forward<Args>(args)...
    };
    this->engaged = true;
  }

  constexpr value_type const& value () const noexcept(false) {
    return *this
      ? **this
      : (throw bad_optional_access { "optional is disengaged" }, **this);
  }

  value_type& value () noexcept(false) {
    if (*this) { return **this; }
    throw bad_optional_access { "optional is disengaged" };
  }

  template <
    class T,
    class=enable_if_t<
      all_traits<
        ::std::is_copy_constructible<value_type>,
        ::std::is_convertible<T, value_type>
      >::value
    >
  > constexpr value_type value_or (T&& val) const& {
    return *this ? **this : static_cast<value_type>(::std::forward<T>(val));
  }

  template <
    class T,
    class=enable_if_t<
      all_traits<
        ::std::is_move_constructible<value_type>,
        ::std::is_convertible<T, value_type>
      >::value
    >
  > value_type value_or (T&& val) && {
    return *this
      ? value_type { ::std::move(**this) }
      : static_cast<value_type>(::std::forward<T>(val));
  }

private:
  constexpr value_type const* ptr (::std::false_type) const noexcept {
    return &this->val;
  }

  value_type const* ptr (::std::true_type) const noexcept {
    return ::std::addressof(this->val);
  }
};
} // namespace butil