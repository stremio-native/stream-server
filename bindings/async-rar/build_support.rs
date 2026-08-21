pub(crate) fn should_compile_unrar_cpp(name: &str, is_windows: bool) -> bool {
    if matches!(
        name,
        // GUI/console entry points are not needed by the library.
        "arccmt.cpp"
            | "consio.cpp"
            | "uiconsole.cpp"
            | "rar.cpp"
            // These sources are included by another compiled translation unit.
            | "blake2s_sse.cpp"
            | "blake2sp.cpp"
            | "unpack15.cpp"
            | "unpack20.cpp"
            | "unpack30.cpp"
            | "unpack50.cpp"
            | "unpack50mt.cpp"
            | "unpack50frag.cpp"
            | "unpackinline.cpp"
            | "coder.cpp"
            | "model.cpp"
            | "suballoc.cpp"
            | "uicommon.cpp"
            | "uisilent.cpp"
            | "crypt1.cpp"
            | "crypt2.cpp"
            | "crypt3.cpp"
            | "crypt5.cpp"
            | "recvol3.cpp"
            | "recvol5.cpp"
            | "rs16.cpp"
            | "cmdfilter.cpp"
            | "cmdmix.cpp"
            | "hardlinks.cpp"
            | "threadmisc.cpp"
            | "win32acl.cpp"
            | "win32stm.cpp"
            | "win32lnk.cpp"
    ) {
        return false;
    }

    if is_windows && matches!(name, "ulinks.cpp" | "uowners.cpp") {
        return false;
    }

    if !is_windows && matches!(name, "threadpool.cpp" | "motw.cpp" | "isnt.cpp") {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::should_compile_unrar_cpp;

    #[test]
    fn sources_included_by_compiled_translation_units_are_not_compiled_twice() {
        assert!(should_compile_unrar_cpp("cmddata.cpp", true));
        assert!(should_compile_unrar_cpp("cmddata.cpp", false));
        assert!(!should_compile_unrar_cpp("cmdfilter.cpp", true));
        assert!(!should_compile_unrar_cpp("cmdfilter.cpp", false));
        assert!(!should_compile_unrar_cpp("cmdmix.cpp", true));
        assert!(!should_compile_unrar_cpp("cmdmix.cpp", false));

        assert!(should_compile_unrar_cpp("extinfo.cpp", true));
        assert!(should_compile_unrar_cpp("extinfo.cpp", false));
        assert!(!should_compile_unrar_cpp("hardlinks.cpp", true));
        assert!(!should_compile_unrar_cpp("hardlinks.cpp", false));

        assert!(should_compile_unrar_cpp("threadpool.cpp", true));
        assert!(!should_compile_unrar_cpp("threadmisc.cpp", true));
        assert!(!should_compile_unrar_cpp("threadpool.cpp", false));
        assert!(!should_compile_unrar_cpp("threadmisc.cpp", false));
    }
}
